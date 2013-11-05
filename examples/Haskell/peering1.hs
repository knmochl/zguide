import System.Environment (getArgs)
import System.Exit (exitWith, ExitCode(..))
import Data.ByteString.Char8 (pack, unpack)
import Control.Monad (forM_, forever)
import Control.Applicative ((<$>))
import Text.Printf
import System.Random (getStdRandom, randomR)
import System.ZMQ3.Monadic

main = do
    args <- getArgs
    if length args < 2 then do
        putStrLn "syntax: peering1 me {you}..."
        exitWith ExitSuccess -- returning 0 seems odd here
    else
        return ()
    let (me:peers) = args
    runZMQ $ do
        stateBackend <- socket Pub
        bind stateBackend $ "ipc://" ++ me ++ "-state.ipc"
        stateFrontend <- socket Sub
        subscribe stateFrontend $ pack ""
        forM_ peers $ \i -> connect stateFrontend $ "ipc://" ++ i ++ "-state.ipc"
        forever $ do
            [evts] <- poll 1000 [Sock stateFrontend [In] Nothing]
            if In `elem` evts then
                processStateMessage stateFrontend
            else do
                sendAvailableMessage stateBackend me

getRandomInt :: (Int,Int) -> IO Int
getRandomInt = getStdRandom . randomR

sendAvailableMessage :: (Sender s) => Socket z s -> String -> ZMQ z ()
sendAvailableMessage backend me = do
    send backend [SendMore] (pack me)
    available <- liftIO $ getRandomInt (1,10)
    send backend [] (pack . show $ available)
    return ()

processStateMessage :: (Receiver r) => Socket z r -> ZMQ z ()
processStateMessage frontend = do
    peer_name <- unpack <$> receive frontend
    clients <- unpack <$> receive frontend
    liftIO $ printf "%s - %s workers free\n" peer_name clients
    return ()
