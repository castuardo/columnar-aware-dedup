# Building

Run mvn package. The output will be created at target/ColumnarAwareDedup. This includes jars, deps and scripts.

# Testing

## Basic test

This is a basic test to check the deployment.

1. Build.
2. Go to target/ColumnarAwareDedup/testmain/scripts.
3. Execute runTestMain.sh 

You should see two log lines. If thats fine, then your setup is correct.

## Simple-client server

This test uses plain streamer/receiver, that send a parquet file from one side to the other in chunks. This is the baseline 
to compare with redundant traffic reduction techniques.

1. Build
2. Go to target/ColumnarAwareDedup/plain/scripts
3. Check server (createServer.sh) script: The default values for the properties will listen at port 2000 of 127.0.0.1 and output 
files to /tmp/server.
4. Check client (createClient.sh) script: The default values for the properties will connect to port 2000 of 127.0.0.1. The default input 
file/dir is /tmp/client (this can be set as either an individual file or a directory). The test also filters to only handle parquet files.
5. For client, you can create /tmp/client folders and add some parquet files to transfer them.
6. Run runTestPlain.sh.

Check the logs (client.log and server.log). All the transferred files will be logged including transferred bytes and transfer time.