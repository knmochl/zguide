import System.Exit (exitWith, ExitCode(..))
import System.Random (getStdRandom, randomR)
import Control.Concurrent (threadDelay)
import Control.Applicative ((<$>))
import Data.ByteString.Char8 (pack, unpack)
import System.ZMQ3.Monadic

getRandomInt :: (Int,Int) -> IO Int
getRandomInt = getStdRandom . randomR

runServer :: (Sender a, Receiver a) => Socket z a -> Int -> ZMQ z ()
runServer server 0 = do
    close server
    liftIO $ exitWith ExitSuccess
runServer server cycles = do
    msg <- unpack <$> receive server
    randomChance <- liftIO $ getRandomInt (0,3)
    if cycles > 3 && randomChance == 0 then do
        liftIO $ putStrLn "I: simulating a crash"
        runServer server 0
    else do
        randomChance' <- liftIO $ getRandomInt (0,3)
        if cycles > 3 && randomChance' == 0 then do
            liftIO $ putStrLn "I: simulating CPU overload"
            liftIO $ threadDelay $ 2 * 1000000
        else do
            return ()
        liftIO $ putStrLn $ "I: normal request " ++ msg
        liftIO $ threadDelay $ 1 * 1000000
        send server [] (pack msg)
    runServer server (cycles + 1)

main = do
    runZMQ $ do
        server <- socket Rep
        bind server "tcp://*:5555"
        runServer server 1