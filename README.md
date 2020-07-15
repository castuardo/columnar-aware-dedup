# Building

Run mvn package. The output will be created at target/ColumnarAwareDedup. This includes jars, deps and scripts.

## Building customized parquet-mr

The original [parquet-mr](https://github.com/apache/parquet-mr/tree/apache-parquet-1.10.0) reads, decodes, and uncompresses data mixedly, and makes it hard to directly use their API to separate metadata and data. I finally decide to change their code to expose usefull information outside. The customized one is [here](https://github.com/YangZhou1997/parquet-mr/tree/speedup). 

To install it in your local respository, 
```bash
# clone customized parquet-mr
git clone git@github.com:YangZhou1997/parquet-mr.git 
cd parquet-mr && git checkout speedup

# install customized version to the local repository
mvn clean install -Drat.numUnapprovedLicenses=200 -DskipTests -pl parquet-column,parquet-hadoop,parquet-cli,parquet-common

# in columnar-aware-dedup rep
mvn clean package

# if the above command shows unknown symbols, you run following to force using only local rep during building. 
mvn clean package -o
```

Tested on Ubuntu 16.04.3 LTS (GNU/Linux 4.4.0-184-generic x86_64) with Apache Maven 3.3.9. 

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

## Naive ORC client-server

This test uses a naive chubking algorithm for ORC files, this is, is interested in trying to deduplicate the whole data and footer section of each 
stripe in the file. As expected, this works fine in databases where, for example, two query results look very similar, or other cases.

1. Build
2. Go to target/ColumnarAwareDedup/orc/naive/scripts
3. Check server (createServer.sh) script: The default values for the properties will listen at port 2000 of 127.0.0.1 and output 
files to /tmp/server.
4. Check client (createClient.sh) script: The default values for the properties will connect to port 2000 of 127.0.0.1. The default input 
file/dir is /tmp/client (this can be set as ither an individual file or a directory). The test also filters to only handle orc files.
5. For client, you can create /tmp/client folders and add some orc files to transfer them. The basic way to test this is to copy the same
orc file twice so you can see how the whole data section is deduplicated.
6. Run runNaiveTestORC.sh.

Check the logs (client.log and server.log). You should see in client.log something like:


file=/tmp/client/o1.orc, stats=[ExtraTransferBytes=98.0 Bytes (5), TransferBytes=1908.0 Bytes (4)]
...
file=/tmp/client/copy-of-o1.orc, stats=[ExtraTransferBytes=91.0 Bytes (5), DedupBytes=1604.0 Bytes (1), TransferBytes=304.0 Byt
es (4)]

Meaning that the size of the data was 1604 bytes and the total bytes transfered through the network was 304 with 91 bytes of overhead.
