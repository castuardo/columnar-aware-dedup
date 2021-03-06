#########
## source the env first
#########
source environment.sh
## server properties, if not set default values will be used...
PROPS=""
## PROPS="-Dcawd.server.host=1270.0.1 $PROPS"
## PROPS="-Dcawd.server.port=2000 $PROPS"
## PROPS="-Dcawd.server.outputFolder=/tmp/server $PROPS"
PROPS="-Dcawd.stripecol.warmupCount=0 $PROPS"
PROPS="-Dcawd.receiver.type=vmware.speedup.cawd.orc.net.StripePlusColumnORCReceiver $PROPS"
## log file...
LOG_PROPS="-Dlog4j.configurationFile=$BASE_DIR/conf/server-default.xml"
## main class
MAIN="vmware.speedup.cawd.main.ParquetServerMain"
## now start the server...
java -cp $MAIN_JAR:$CLASS_PATH $PROPS $LOG_PROPS $MAIN &
