#########
## source the env first
#########
source environment.sh
## start the server
echo "Starting server..."
sleep 5
bash startServer.sh
echo "Starting client..."
bash startClient.sh
echo "Done, check results in output folder and logs..."
## done, now check results...