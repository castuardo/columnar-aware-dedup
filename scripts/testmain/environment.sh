#!/bin/bash

#############
## add some general properties here
#############
export BASE_DIR="../.."
export MAIN_JAR="$BASE_DIR/ColumnarAwareDedup.jar"
export CLASS_PATH="$BASE_DIR/deps/*"
export LOG_PROPS="-Dlog4j.configurationFile=$BASE_DIR/conf/log4j-default.xml"

