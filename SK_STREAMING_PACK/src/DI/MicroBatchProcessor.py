"""
# 의존성 주입으로 해당 processor 함수에서 전처리를 발생합니다.
# df 는 kafka topic 에서 받아온 json 데이터를 dataframe 화 한 변수 입니다.
"""
from src.DI.Model.SparkModel import OracleModel, SparkModel
from src.DI.Util.Preprocess import *

def MicroBatchProcessor(df, batch_id, spark):
    """
    # 스파크 스트리밍 DI 함수
    """

    df.show()
