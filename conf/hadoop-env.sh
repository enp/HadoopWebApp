#!/usr/bin/env sh
										

export HDFS_DATANODE_OPTS="-Xms700m -Xmx8G"
export HDFS_JOURNALNODE_OPTS="-Xms700m -Xmx8G"
export HDFS_NAMENODE_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=1026 -Xms1G -Xmx8G"
export HDFS_ZKFC_OPTS="-Xms500m -Xmx8G"
