#!/bin/sh

echo 'Running ' $ACCUMULO_HOME/bin/tool.sh target/accumulo-sort-0.0.1-SNAPSHOT.jar sorts.mapred.MediawikiIngestJob -libjars $HADOOP_PREFIX/lib/commons-configuration-1.6.jar,$HADOOP_PREFIX/lib/commons-collections-3.2.1.jar,/home/elserj/.m2/repository/javax/xml/bind/jaxb-api/2.2.9/jaxb-api-2.2.9.jar,/home/elserj/.m2/repository/com/sun/xml/bind/jaxb-impl/2.2.7-b63/jaxb-impl-2.2.7-b63.jar,/home/elserj/.m2/repository/com/sun/xml/bind/jaxb-core/2.2.7-b63/jaxb-core-2.2.7-b63.jar

$ACCUMULO_HOME/bin/tool.sh target/accumulo-sort-0.0.1-SNAPSHOT.jar sorts.mapred.MediawikiIngestJob -libjars $HADOOP_PREFIX/lib/commons-configuration-1.6.jar,$HADOOP_PREFIX/lib/commons-collections-3.2.1.jar,/home/elserj/.m2/repository/javax/xml/bind/jaxb-api/2.2.9/jaxb-api-2.2.9.jar,/home/elserj/.m2/repository/com/sun/xml/bind/jaxb-impl/2.2.7-b63/jaxb-impl-2.2.7-b63.jar,/home/elserj/.m2/repository/com/sun/xml/bind/jaxb-core/2.2.7-b63/jaxb-core-2.2.7-b63.jar
