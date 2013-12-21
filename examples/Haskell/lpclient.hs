import System.Exit (exitWith, ExitCode(..))
import Data.ByteString.Char8 (pack, unpack)
import Control.Applicative ((<$>))
import System.ZMQ3.Monadic

requestTimeout = 2000
requestRetries = 3
serverEndpoint = "tcp://localhost:5555"

runClient :: (Sender a, Receiver a) => Socket z a -> Int -> Int -> ZMQ z ()
runClient client sequence retriesLeft = do
    send client [] (pack . show $ sequence)
    getReply client sequence retriesLeft

getReply :: (Sender a, Receiver a) => Socket z a -> Int -> Int -> ZMQ z ()
getReply client _ 0 = do
    liftIO $ putStrLn "E: server seems to be offline, abandoning"
    close client
    return ()
getReply client sequence retriesLeft = do
    [evts] <- poll requestTimeout [Sock client [In] Nothing]
    if In `elem` evts then do
        reply <- unpack <$> receive client
        if reply == (show sequence) then do
            liftIO $ putStrLn $ "I: server replied OK ( " ++ reply ++ ")"
            runClient client (sequence + 1) requestRetries
        else do
            liftIO $ putStrLn $ "E: malformed reply from server: " ++ reply
            getReply client sequence retriesLeft
    else do
        liftIO $ putStrLn "W: no response from server, retrying..."
        close client
        liftIO $ putStrLn "I: reconnecting to server..."
        startClient sequence (retriesLeft - 1)
        
startClient :: Int -> Int -> ZMQ z ()
startClient sequence retriesLeft = do
    client <- socket Req
    connect client serverEndpoint
    runClient client sequence retriesLeft

main = do
    runZMQ $ do
        liftIO $ putStrLn "I: connecting to server..."
        startClient 1 requestRetries
