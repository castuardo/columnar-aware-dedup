#########
## source the env first
#########
source environment.sh
## start the server
echo "Starting server..."
bash startServer.sh &
sleep 5
echo "Starting client..."
bash startClient.sh &
echo "Done, check results in output folder and logs..."
# wait all bg processes
wait
## done, now check results...