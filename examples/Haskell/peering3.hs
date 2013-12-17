import System.Environment (getArgs)
import System.Exit (exitWith, ExitCode(..))
import Data.ByteString.Char8 (pack, unpack)
import Data.Char (chr)
import Data.List.NonEmpty (fromList)
import Control.Monad (forM_, forever, when, replicateM_, replicateM)
import Control.Applicative ((<$>))
import Control.Concurrent (threadDelay)
import Text.Printf
import System.Random (getStdRandom, randomR)
import System.ZMQ3.Monadic
import Control.Monad.Reader
import Control.Monad.State

type Worker = String
type Broker = String

data AppConfig z router sub pub pull = AppConfig {
      name :: Broker
    , peerList :: [Broker]
    , localFE :: Socket z router
    , localBE :: Socket z router
    , cloudFE :: Socket z router
    , cloudBE :: Socket z router
    , stateFE :: Socket z sub
    , stateBE :: Socket z pub
    , mon     :: Socket z pull
}

data AppState = AppState {
      workerList :: [Worker]
    , cloudCapacity :: Int
}


workerReady :: String
workerReady = [chr 1]

numWorkers :: Int
numWorkers = 5

numClients :: Int
numClients = 10

getRandomInt :: (Int,Int) -> IO Int
getRandomInt = getStdRandom . randomR

receiveMessage :: (Receiver a) => Socket z a -> ZMQ z [String]
receiveMessage sock = map unpack <$> receiveMulti sock

appReceiveMessage :: (Receiver a) => Socket z a -> App z router sub pub pull [String]
appReceiveMessage = lift . lift . receiveMessage

unwrapMessage :: [String] -> (String, [String])
unwrapMessage msg = let msg' = tail msg
                        target = head msg in
                      if head msg' == [] then
                          (target, tail msg')
                      else
                          (target, msg')

sendToWorker :: (Sender a) => Socket z a -> Worker -> [String] -> ZMQ z ()
sendToWorker sock worker msg = sendMulti sock $ fromList $ map pack $ worker : "" : msg

appSendToWorker :: (Sender a) => Socket z a -> Worker -> [String] -> App z router sub pub pull ()
appSendToWorker sock worker msg = lift (lift (sendToWorker sock worker msg))

startClientTask :: Broker -> ZMQ z ()
startClientTask broker = do
    client <- socket Req
    connect client $ "ipc://" ++ broker ++ "-localfe.ipc"
    monitor <- socket Push
    connect monitor $ "ipc://" ++ broker ++ "-monitor.ipc"
    clientTask client monitor

clientTask :: (Receiver a, Sender a, Sender b) => Socket z a -> Socket z b -> ZMQ z ()
clientTask client monitor = do
    sleepTime <- liftIO $ getRandomInt (1,5)
    messageCount <- liftIO $ getRandomInt (1,15)
    liftIO $ threadDelay $ sleepTime * 1000
    statuses <- replicateM messageCount $ do
        randomTask <- liftIO $ getRandomInt (0, 16 * 16 * 16 * 16)
        let taskId = printf "%04X" randomTask
        send client [] $ pack taskId
        [evts] <- poll (10 * 1000) [Sock client [In] Nothing]
        if In `elem` evts then do
            msg <- receiveMessage client
            if taskId == (head msg) then do
                send monitor [] $ pack $ head msg
                return True
            else do
                send monitor [] $ pack $ "Wrong task: " ++ (head msg)
                return False
        else do
            send monitor [] $ pack $ "Lost task: " ++ taskId
            return False
    if False `elem` statuses then
        return ()
    else
        clientTask client monitor

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

routeMessage :: (Sender a) => [Broker] -> Socket z a -> Socket z a -> [String] -> ZMQ z ()
routeMessage peers localFront cloudFront msg = do
    let msg' = map pack $ msg
    if (head msg `elem` peers) then do
        sendMulti cloudFront (fromList msg')
    else do
        sendMulti localFront (fromList msg')

--routeClients = undefined
routeClients :: (Receiver pull, Receiver router, Sender router, Receiver sub, Sender pub) => App z router sub pub pull ()
routeClients = do
    cfg <- ask
    st <- get
    let workers = workerList st
        peers = peerList cfg
        cloud = cloudCapacity st
    if (length workers > 0 || cloud > 0) then do
        let pollList = Sock (localFE cfg) [In] Nothing : if workers == [] then [] else [Sock (cloudFE cfg) [In] Nothing]
        evtsList <- poll 0 pollList
        if In `elem` (evtsList !! 0) then do
            msg <- appReceiveMessage (localFE cfg)
            appSendToWorker (localBE cfg) (head workers) msg
            put st { workerList = (tail workers), cloudCapacity = cloud }
            routeClients
        else
            if (length evtsList > 1) && (In `elem` (evtsList !! 1)) then do
                msg <- appReceiveMessage (cloudFE cfg)
                randomPeer <- liftIO $ getRandomInt (1, length peers)
                let (dest, sock, newWorkers, newCloud) = if workers == [] then
                            (peers !! (randomPeer - 1), cloudBE cfg, workers, cloud - 1)
                        else
                            (head workers, localBE cfg, tail workers, cloud)
                appSendToWorker sock dest msg
                put st { workerList = newWorkers, cloudCapacity = newCloud }
                routeClients
            else return ()
    else return ()

handleCloud evts = undefined
--handleCloud :: (Receiver pull, Receiver router, Sender router, Receiver sub, Sender pub) => [Broker] -> [Worker] -> SocketGroup z router sub pub pull -> [Event] -> ZMQ z ()
--handleCloud peers workers sockets evts = do
--    when (In `elem` evts) $ do
--        msg <- receiveMessage (cloudBE sockets)
--        let (_, msg') = unwrapMessage msg
--        routeMessage peers (localFE sockets) (cloudFE sockets) msg'
--        routeTraffic peers workers sockets
--
handleLocal evts = undefined
--handleLocal :: (Receiver pull, Receiver router, Sender router, Receiver sub, Sender pub) => [Broker] -> [Worker] -> SocketGroup z router sub pub pull -> [Event] -> ZMQ z ()
--handleLocal peers workers sockets evts = do
--    when (In `elem` evts) $ do
--        msg <- receiveMessage (localBE sockets)
--        let (workerName, msg') = unwrapMessage msg
--        when ((head msg') /= workerReady) $ do
--            routeMessage peers (localFE sockets) (cloudFE sockets) msg'
--        let workerList = workers ++ [workerName]
--        newWorkers <- routeClients peers workerList 0 sockets
--        updateCloud (length workerList) (length newWorkers) (stateBE sockets)
--        routeTraffic peers newWorkers sockets
--
--handleState evts = undefined
handleState :: (Receiver pull, Receiver router, Sender router, Receiver sub, Sender pub) => [Event] -> App z router sub pub pull ()
handleState evts = do
    when (In `elem` evts) $ do
        cfg <- ask
        st <- get
        msg <- appReceiveMessage (stateFE cfg)
        let (_, msg') = unwrapMessage msg
        let cloudCount = read . head $ msg'
            workers = workerList st
        put st { workerList = workers, cloudCapacity = cloudCount }
        routeClients
        st' <- get
        let newWorkers = workerList st'
        updateCloud (length workers) (length newWorkers) (stateBE cfg)
        routeTraffic

--updateCloud a b c = undefined
updateCloud :: (Sender a) => Int -> Int -> Socket z a -> App z router sub pub pull ()
updateCloud old new sock = when (old /= new) $ do
    cfg <- ask
    appSendToWorker sock (name cfg) [show new]
--
--handleMon evts = undefined
--handleMon :: (Receiver pull, Receiver router, Sender router, Receiver sub, Sender pub) => [Broker] -> [Worker] -> SocketGroup z router sub pub pull -> [Event] -> ZMQ z ()

handleMon :: (Receiver pull, Receiver router, Sender router, Receiver sub, Sender pub) => [Event] -> App z router sub pub pull ()
handleMon evts = do
    when (In `elem` evts) $ do
        cfg <- ask
        msg <- appReceiveMessage (mon cfg)
        liftIO $ putStrLn (head msg)
        routeTraffic

type App z router sub pub pull = ReaderT (AppConfig z router sub pub pull) (StateT AppState (ZMQ z))

routeTraffic :: (Receiver pull, Receiver router, Sender router, Sender pub, Receiver sub) => App z router sub pub pull ()
routeTraffic = do
    cfg <- ask
    st <- get
    let workers = workerList st
        peers = peerList cfg
        timeout = if workers == [] then (-1) else 1000
    poll timeout [Sock (localBE cfg) [In] (Just handleLocal),
                  Sock (cloudBE cfg) [In] (Just handleCloud),
                  Sock (stateFE cfg) [In] (Just handleState),
                  Sock (mon cfg) [In] (Just handleMon)]
    routeClients
    st' <- get
    let newWorkers = workerList st'
    updateCloud (length workers) (length newWorkers) (stateBE cfg)
    routeTraffic

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
        replicateM_ numWorkers (async $ workerTask me)
        replicateM_ numClients (async $ startClientTask me)
        let config = AppConfig me peers localFront localBack cloudFront cloudBack stateFront stateBack monitor
            state = AppState [] 0
        runStateT (runReaderT routeTraffic config) state
        liftIO $ putStrLn "foo"
