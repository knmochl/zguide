import System.Environment (getArgs)
import System.Exit (exitWith, ExitCode(..))
import Data.ByteString.Char8 (pack, unpack)
import Data.Char (chr)
import Data.List.NonEmpty (fromList)
import Control.Monad (forM_, forever, when)
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

receiveMessage :: (Receiver a) => Socket z a -> ZMQ z [String]
receiveMessage sock = map unpack <$> receiveMulti sock

clientTask :: Broker -> ZMQ z ()
clientTask broker = do
    client <- socket Req
    connect client $ "ipc://" ++ broker ++ "-localfe.ipc"
    forever $ do
        send client [] $ pack "HELLO"
        receive client >>= \msg -> liftIO $ putStrLn $ unwords ["Client:", (unpack msg)]
        liftIO $ threadDelay $ 1 * 1000

workerTask :: Broker -> ZMQ z ()
workerTask broker = do
    worker <- socket Req
    connect worker $ "ipc://" ++ broker ++ "-localbe.ipc"
    send worker [] $ pack $ [chr 1]
    forever $ do
        receive worker >>= \msg -> liftIO $ putStrLn $ unwords ["Worker:", (unpack msg)]
        send worker [] $ pack "OK"

handleCloud :: (Receiver a, Sender a) => [Broker] -> SocketGroup z a -> [Event] -> ZMQ z ()
handleCloud peers sockets evts = do
    when (In `elem` evts) $ do
        msg <- map unpack <$> receiveMulti (cloudBE sockets)
        let msg' = tail msg
        routeMessage peers (localFE sockets) (cloudFE sockets) msg'
        return ()

handleLocal :: (Receiver a, Sender a) => [Broker] -> [Worker] -> SocketGroup z a -> [Event] -> ZMQ z ()
handleLocal peers workers sockets evts = do
    when (In `elem` evts) $ do
        msg <- map unpack <$> receiveMulti (localBE sockets)
        let workerName = head msg
            msg' = tail msg
        when (head msg' /= [chr 1]) $ do
            routeMessage peers (localFE sockets) (cloudFE sockets) msg'
        routeClients peers workers sockets
        return ()

routeClients :: (Receiver a, Sender a) => [Broker] -> [Worker] -> SocketGroup z a -> ZMQ z ()
routeClients peers [] sockets = return ()
routeClients peers workers sockets = do
    [evtsL, evtsC] <- poll 1000 [Sock (localFE sockets) [In] Nothing,
                                 Sock (cloudFE sockets) [In] Nothing]
    if In `elem` evtsC then do
        msg <- receiveMessage (cloudFE sockets)
        sendToWorker (localBE sockets) (head workers) msg
        routeClients peers (tail workers) sockets
    else
        return ()

sendToWorker :: (Sender a) => Socket z a -> Worker -> [String] -> ZMQ z ()
sendToWorker sock worker msg = sendMulti sock $ fromList $ map pack $ worker : msg

routeMessage :: (Sender a) => [Broker] -> Socket z a -> Socket z a -> [String] -> ZMQ z ()
routeMessage peers localFront cloudFront msg = do
    let msg' = map pack $ msg
    if (head msg `elem` peers) then
        sendMulti cloudFront (fromList msg')
    else
        sendMulti localFront (fromList msg')

routeTraffic :: (Receiver a, Sender a) => [Worker] -> [Broker] -> SocketGroup z a -> ZMQ z ()
routeTraffic workers peers sockets = forever $ do
    let localBack  = localBE sockets
        cloudBack  = cloudBE sockets
    poll 1000 [Sock localBack [In] (Just $ handleLocal peers workers sockets),
               Sock cloudBack [In] (Just $ handleCloud peers sockets)]

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
        forM_ peers $ \i -> connect cloudBack $ "ipc://" ++ i ++ "-cloud.ipc"
        localFront <- socket Router
        bind localFront $ "ipc://" ++ me ++ "-localfe.ipc"
        localBack <- socket Router
        bind localBack $ "ipc://" ++ me ++ "-localbe.ipc"
        let sockets = SocketGroup localFront localBack cloudFront cloudBack
        liftIO $ putStrLn "Press Enter when all brokers are started: "
        _ <- liftIO $ getLine
        routeTraffic [] peers sockets

        liftIO $ putStrLn "Done" 
