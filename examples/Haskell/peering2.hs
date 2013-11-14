import System.Environment (getArgs)
import System.Exit (exitWith, ExitCode(..))
import Data.ByteString.Char8 (pack, unpack)
import Data.Char (chr)
import Control.Monad (forM_, forever, when)
import Control.Applicative ((<$>))
import Control.Concurrent (threadDelay)
import Text.Printf
import System.Random (getStdRandom, randomR)
import System.ZMQ3.Monadic

clientTask :: String -> ZMQ z ()
clientTask broker = do
    client <- socket Req
    connect client $ "ipc://" ++ broker ++ "-localfe.ipc"
    forever $ do
        send client [] $ pack "HELLO"
        receive client >>= \msg -> liftIO $ putStrLn $ unwords ["Client:", (unpack msg)]
        liftIO $ threadDelay $ 1 * 1000

workerTask :: String -> ZMQ z ()
workerTask broker = do
    worker <- socket Req
    connect worker $ "ipc://" ++ broker ++ "-localbe.ipc"
    send worker [] $ pack $ [chr 1]
    forever $ do
        receive worker >>= \msg -> liftIO $ putStrLn $ unwords ["Worker:", (unpack msg)]
        send worker [] $ pack "OK"

handleCloud :: (Receiver a) => Socket a -> [Event] -> ZMQ z ()
handleCloud sock evts = do
    when (In `elem` evts) $ do
        msg <- unpack <$> receive sock
        return ()

handleLocal :: (Receiver a) => Socket a -> [Event] -> ZMQ z ()
handleLocal sock evts = do
    when (In `elem` evts) $ do
        msg <- unpack <$> receive sock
        return ()

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
        --setIdentity (pack me) cloudFront
        bind cloudFront $ "ipc://" ++ me ++ "-cloud.ipc"
        cloudBack <- socket Router
        forM_ peers $ \i -> connect cloudBack $ "ipc://" ++ i ++ "-cloud.ipc"
        localFront <- socket Router
        bind localFront $ "ipc://" ++ me ++ "-localfe.ipc"
        localBack <- socket Router
        bind localBack $ "ipc://" ++ me ++ "-localbe.ipc"
        liftIO $ putStrLn "Press Enter when all brokers are started: "
        _ <- liftIO $ getLine
        forever $ do
            poll 1000 [Sock localBack [In] Just $ handleLocal localBack,
                       Sock cloudBack [In] Just $ handleCloud cloudBack]

        liftIO $ putStrLn "Done" 
