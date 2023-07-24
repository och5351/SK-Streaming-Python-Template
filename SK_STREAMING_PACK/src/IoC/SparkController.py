from src.IoC.Model.SparkModel import *
import datetime

class SparkController():

    def sparkStarter(self):

        sparkModel = SparkModel()

        # 스파크 스트리밍용 세션 생성
        spark = sparkModel.createSession(f"[DIP] Spark Image Anaylst Streaming TEST")
        # 스파크 전처리 용 세션 생성
        spark2 = sparkModel.createSession(f"[DIP] Saprk Image Anaylst Preprocessing Test")

        # 카프카 스트리밍 세션 생성
        kafka_read_streaming = sparkModel.kafka_read_stream_session(spark)

        # 토픽 json 파싱
        kafka_json_parsed = sparkModel.kafka_json_value_parser(kafka_read_streaming)

        # 카프카 스트리밍 시작
        sparkModel.kafka_streaming_starter(kafka_json_parsed, spark2)