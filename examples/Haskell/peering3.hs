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
    , stateFE :: Socket z a
    , stateBE :: Socket z a
    , monitor :: Socket z a
}

workerReady :: String
workerReady = [chr 1]

numWorkers :: Int
numWorkers = 10

numClients :: Int
numClients = 5

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
    monitor <- socket Push
    connect monitor $ "ipc://" ++ broker ++ "-monitor.ipc"

    forever $ do
        sleepTime <- liftIO $ getRandomInt (1,5)
        messageCount <- liftIO $ getRandomInt (1,15)
        liftIO $ threadDelay $ sleepTime * 1000
        replicateM_ messageCount $ do
            randomTask <- liftIO $ getRandomInt (0, 16 * 16 * 16 * 16)
            let taskId = printf "%04X" randomTask
            send client [] $ pack taskId
            [evts] <- poll (10 * 1000) [Sock client [In] Nothing]
            if In `elem` evts then do
                reply <- receive client
                send client [] reply
            else do
                send monitor [] $ pack $ "E: CLIENT EXIT - lost task " ++ taskId

workerTask :: Broker -> ZMQ z ()
workerTask broker = do
    worker <- socket Req
    connect worker $ "ipc://" ++ broker ++ "-localbe.ipc"
    send worker [] $ pack workerReady
    forever $ do
        msg <- receiveMessage worker
        let (target, msg') = unwrapMessage msg
        sleepTime <- liftIO $ getRandomInt (0,1)
        liftIO $ threadDelay $ sleepTime * 1000
        sendToWorker worker target msg'

routeTraffic :: (Receiver a, Sender a) => [Broker] -> [Worker] -> SocketGroup z a -> ZMQ z ()
routeTraffic peers workers sockets = do
    let timeout = if workers == [] then (-1) else 1000
    poll timeout [Sock (localBE sockets) [In] Nothing,
                  Sock (cloudBE sockets) [In] Nothing,
                  Sock (stateFE sockets) [In] Nothing,
                  Sock (monitor sockets) [In] Nothing]

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
        localFront <- socket Router
        bind localFront $ "ipc://" ++ me ++ "-localfe.ipc"
        localBack <- socket Router
        bind localBack $ "ipc://" ++ me ++ "-localbe.ipc"
        cloudFront <- socket Router
        setIdentity (restrict $ pack me) cloudFront
        bind cloudFront $ "ipc://" ++ me ++ "-cloud.ipc"
        cloudBack <- socket Router
        setIdentity (restrict $ pack me) cloudBack
        stateBack <- socket Pub
        bind stateBack $ "ipc://" ++ me ++ "-state.ipc"
        stateFront <- socket Sub
        subscribe stateFront $ pack ""
        forM_ peers $ \i -> do
            liftIO $ putStrLn $ "I: connecting to cloud frontend at " ++ i
            connect cloudBack $ "ipc://" ++ i ++ "-cloud.ipc"
            liftIO $ putStrLn $ "I: connecting to state backend at " ++ i
            connect stateFront $ "ipc://" ++ i ++ "-state.ipc"
        monitor <- socket Pull
        bind monitor $ "ipc://" ++ me ++ "-monitor.ipc"
        let sockets = SocketGroup localFront localBack cloudFront cloudBack stateFront stateBack monitor
        replicateM_ numWorkers (async $ clientTask me)
        replicateM_ numClients (async $ workerTask me)
        --routeTraffic peers [] sockets
