import System.Environment (getArgs)
import System.Exit (exitWith, ExitCode(..))
import Data.ByteString.Char8 (pack, unpack)
import Data.Char (chr)
import Data.List.NonEmpty (fromList)
import Control.Monad (forM_, forever, when, replicateM_)
import Control.Applicative ((<$>))
import Control.Concurrent (threadDelay)
import Text.Printf
import System.Random (getStdRandom, randomR)
import System.ZMQ3.Monadic

type Worker = String
type Broker = String

data SocketGroup z a = SocketGroup {
      localFE :: Socket z a
    , localBE :: Socket z a
    , cloudFE :: Socket z a
    , cloudBE :: Socket z a
}

workerReady :: String
workerReady = [chr 1]

getRandomInt :: (Int,Int) -> IO Int
getRandomInt = getStdRandom . randomR

receiveMessage :: (Receiver a) => Socket z a -> ZMQ z [String]
receiveMessage sock = map unpack <$> receiveMulti sock

unwrapMessage :: [String] -> (String, [String])
unwrapMessage msg = let msg' = tail msg
                        target = head msg in
                      if head msg' == [] then
                          (target, tail msg')
                      else
                          (target, msg')

sendToWorker :: (Sender a) => Socket z a -> Worker -> [String] -> ZMQ z ()
sendToWorker sock worker msg = sendMulti sock $ fromList $ map pack $ worker : "" : msg

clientTask :: Broker -> ZMQ z ()
clientTask broker = do
    client <- socket Req
    connect client $ "ipc://" ++ broker ++ "-localfe.ipc"
    forever $ do
        send client [] $ pack "HELLO"
        receiveMessage client >>= \msg -> liftIO $ putStrLn $ unwords $ "Client:" : msg
        liftIO $ threadDelay $ 1 * 1000

workerTask :: Broker -> ZMQ z ()
workerTask broker = do
    worker <- socket Req
    connect worker $ "ipc://" ++ broker ++ "-localbe.ipc"
    send worker [] $ pack $ workerReady
    forever $ do
        msg <- receiveMessage worker
        let (target, msg') = unwrapMessage msg
        liftIO $ putStrLn $ "Worker: " ++ last msg'
        sendToWorker worker target (init msg' ++ ["OK"])

handleCloud :: (Receiver a, Sender a) => [Broker] -> [Worker] -> SocketGroup z a -> [Event] -> ZMQ z ()
handleCloud peers workers sockets evts = do
    when (In `elem` evts) $ do
        msg <- receiveMessage (cloudBE sockets)
        let (_, msg') = unwrapMessage msg
        routeMessage peers (localFE sockets) (cloudFE sockets) msg'
        routeTraffic peers workers sockets

handleLocal :: (Receiver a, Sender a) => [Broker] -> [Worker] -> SocketGroup z a -> [Event] -> ZMQ z ()
handleLocal peers workers sockets evts = do
    when (In `elem` evts) $ do
        msg <- receiveMessage (localBE sockets)
        let (workerName, msg') = unwrapMessage msg
        when ((head msg') /= workerReady) $ do
            routeMessage peers (localFE sockets) (cloudFE sockets) msg'
        newWorkers <- routeClients peers (workers ++ [workerName]) sockets
        routeTraffic peers newWorkers sockets

routeClients :: (Receiver a, Sender a) => [Broker] -> [Worker] -> SocketGroup z a -> ZMQ z [Worker]
routeClients peers [] sockets = return []
routeClients peers workers sockets = do
    [evtsL, evtsC] <- poll 1000 [Sock (localFE sockets) [In] Nothing,
                                 Sock (cloudFE sockets) [In] Nothing]
    if In `elem` evtsC then do
        msg <- receiveMessage (cloudFE sockets)
        sendToWorker (localBE sockets) (head workers) msg
        routeClients peers (tail workers) sockets
    else
        if In `elem` evtsL then do
            msg <- receiveMessage (localFE sockets)
            randomChance <- liftIO $ getRandomInt (1,5)
            randomPeer <- liftIO $ getRandomInt (1, length peers)
            let (dest, sock, newWorkers) = if randomChance == 1 then
                        (peers !! (randomPeer - 1), cloudBE sockets, workers)
                    else
                        (head workers, localBE sockets, tail workers)
            sendToWorker sock dest msg
            routeClients peers newWorkers sockets
        else return workers

routeMessage :: (Sender a) => [Broker] -> Socket z a -> Socket z a -> [String] -> ZMQ z ()
routeMessage peers localFront cloudFront msg = do
    let msg' = map pack $ msg
    if (head msg `elem` peers) then do
        sendMulti cloudFront (fromList msg')
    else do
        sendMulti localFront (fromList msg')

routeTraffic :: (Receiver a, Sender a) => [Broker] -> [Worker] -> SocketGroup z a -> ZMQ z ()
routeTraffic peers workers sockets = do
    let localBack  = localBE sockets
        cloudBack  = cloudBE sockets
    poll 1000 [Sock localBack [In] (Just $ handleLocal peers workers sockets),
               Sock cloudBack [In] (Just $ handleCloud peers workers sockets)]
    newWorkers <- routeClients peers workers sockets
    routeTraffic peers newWorkers sockets

main = do
    args <- getArgs
    if length args < 2 then do
        putStrLn "syntax: peering1 me {you}..."
        exitWith ExitSuccess -- returning 0 seems odd here
    else
        return ()
    let (me:peers) = args
    putStrLn $ printf "I: preparing broker at %s..." me
    runZMQ $ do
        cloudFront <- socket Router
        setIdentity (restrict $ pack me) cloudFront
        bind cloudFront $ "ipc://" ++ me ++ "-cloud.ipc"
        cloudBack <- socket Router
        setIdentity (restrict $ pack me) cloudBack
        forM_ peers $ \i -> connect cloudBack $ "ipc://" ++ i ++ "-cloud.ipc"
        localFront <- socket Router
        bind localFront $ "ipc://" ++ me ++ "-localfe.ipc"
        localBack <- socket Router
        bind localBack $ "ipc://" ++ me ++ "-localbe.ipc"
        let sockets = SocketGroup localFront localBack cloudFront cloudBack
        liftIO $ putStrLn "Press Enter when all brokers are started: "
        _ <- liftIO $ getLine
        replicateM_ 10 (async $ clientTask me)
        replicateM_ 3 (async $ workerTask me)
        routeTraffic peers [] sockets
