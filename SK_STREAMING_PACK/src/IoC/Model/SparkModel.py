from src.DI.MicroBatchProcessor import MicroBatchProcessor
from pyspark.sql import SparkSession
from src.DI.Util.SchemaRegistry import *
from src.DI.config import ConfigMaster


class SparkModel:

    def createSession(self, appName: str) -> SparkSession:
        """
        # Spark session 선언
        # appName -> str : SparkSession의 App 이름 변수
        # return : pyspark.sql.SparkSession
        """
        return SparkSession.builder \
            .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
            .config('partitionoverwritemode', 'dynamic') \
            .appName(appName).enableHiveSupport().getOrCreate()

    def kafka_read_stream_session(self, spark: SparkSession):
        """
        # kafka 의 지정된 클러스터, 토픽에서 stream 을 실시간으로 읽어오는 함수
        """
        configMaster = ConfigMaster()

        return spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", configMaster.get_kafka_sink_cluster()) \
            .option("subscribe", configMaster.get_kafka_sink_topic()) \
            .option("startingOffsets", configMaster.get_kafka_sink_start_offset()) \
            .option("kafka.group.id", configMaster.get_kafka_group_id()) \
            .load()

    def kafka_json_value_parser(self, spark: SparkSession):
        """
        # kafka 에서 읽어온 json value 를 테이블화 시키는 함수
        """

        return spark.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    def kafka_streaming_starter(self, spark: SparkSession, preproces_session: SparkSession):
        """
        # 카프카 스트리밍 실행 함수 
        """
        query = spark \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(lambda df, batch_id: MicroBatchProcessor(df, batch_id, preproces_session)) \
            .start()

        query.awaitTermination()
