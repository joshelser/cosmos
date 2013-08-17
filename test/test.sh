. ~/classpath.sh
. /opt/hadoop/conf/hadoop-env.sh
echo java -cp ${CLASSPATH}:target/cosmos-test-0.1.0.jar cosmos.mapred.MediawikiIngestJob
CLASSPATH=${CLASSPATH}:../core/target/cosmos-core-0.1.0.jar
java -cp ${CLASSPATH}:target/cosmos-test-0.1.0.jar cosmos.MediawikiQueries
