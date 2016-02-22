source ./sparkstreaming.config

JAR_FILE="./target/scala-2.10/wikimediabot-assembly-1.0.jar"
CLASS_NAME="WikimediaBotReader"

ZOOKEEPER="localhost:2181"

KAFKA_GROUP="SparkKafkaTest"
KAFKA_TOPICS="en.wikipedia,en.wiktionary,en.wikinews,commons.wikimedia"
KAFKA_PARTITIONS="1"

echo "Submitting spark streaming job"

time ${SPARK_HOME}/spark-submit --class ${CLASS_NAME} --master ${SPARK_MASTER} ${JAR_FILE} ${ZOOKEEPER} ${KAFKA_GROUP} ${KAFKA_TOPICS} ${KAFKA_PARTITIONS}
