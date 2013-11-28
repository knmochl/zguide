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

workerReady :: String
workerReady = [chr 1]

getRandomInt :: (Int,Int) -> IO Int
getRandomInt = getStdRandom . randomR

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
        receiveMessage client >>= \msg -> liftIO $ putStrLn $ unwords $ "Client:" : msg

