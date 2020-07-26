#########
## source the env first
#########
source environment.sh
## This is the main class here, no params...
MAIN="vmware.speedup.cawd.main.AlluxioSim"
## now run it
java -cp $MAIN_JAR:$CLASS_PATH $LOG_PROPS $MAIN

