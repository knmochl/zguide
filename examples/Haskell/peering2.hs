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
        liftIO $ putStr "Press Enter when all brokers are started: "
        liftIO $ getLine
 
