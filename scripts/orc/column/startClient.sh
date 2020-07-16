#########
## source the env first
#########
source environment.sh
## client properties, if not set default values will be used...
PROPS=""
## PROPS="-Dcawd.client.host=1270.0.1 $PROPS"
## PROPS="-Dcawd.client.port=2000 $PROPS"
## PROPS="-Dcawd.client.input=/tmp/client $PROPS"
PROPS="-Dcawd.client.filters=.orc $PROPS"
PROPS="-Dcawd.streamer.type=vmware.speedup.cawd.orc.net.ColumnBasedORCStreamer $PROPS"
## log file...
LOG_PROPS="-Dlog4j.configurationFile=$BASE_DIR/conf/client-default.xml"
## main class
MAIN="vmware.speedup.cawd.main.ParquetClientMain"
## now start the client...
java -cp $MAIN_JAR:$CLASS_PATH $PROPS $LOG_PROPS $MAIN &
