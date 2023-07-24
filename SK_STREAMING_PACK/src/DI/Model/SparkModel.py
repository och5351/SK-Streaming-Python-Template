from pyspark.sql import SparkSession


class SparkModel:
    """
    # 스파크 관련 메소드 모음 클래스
    """

    @staticmethod
    def createSession(appName: str) -> SparkSession:
        """
        # Spark session 선언
        # appName -> str : SparkSession의 App 이름 변수
        # return : pyspark.sql.SparkSession
        """
        return SparkSession.builder \
                .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
                .config('partitionoverwritemode', 'dynamic') \
                .appName(appName).enableHiveSupport().getOrCreate()


class OracleModel:
    """
    # Oracle 관련 메소드 모음 클래스
    # Spark 를 활용하여 Oracle 을 조회 합니다.
    """

    def __init__(self):
        from src.DI.config import ConfigMaster
        self.configMaster = ConfigMaster()

    def read_oracle_password(self, spark: SparkSession):
        '''
        # Hadoop Credential 을 적용시킨 Oracle 비밀번호를 가져오는 함수 입니다.
        # return str
        '''
        jceks_path = self.configMaster.get_oracle_jceks()
        spark_config = spark.sparkContext._jsc.hadoopConfiguration()
        spark_config.set(
            "hadoop.security.credential.provider.path", jceks_path)
        jceks = spark_config.getPassword(
            self.configMaster.get_oracle_jceks_alias())

        return ''.join([str(s) for s in jceks])

    def oracle_select(self, spark: SparkSession, sql: str):
        """
        # Oracle 에서 select 를 해줄 수 있습니다.
        """
        return spark.read \
                    .format("jdbc") \
                    .option("url", self.configMaster.get_oracle_jceks_alias()) \
                    .option("user", "DATUSR") \
                    .option("password", self.read_oracle_password(spark)) \
                    .option("query", sql) \
                    .load()

    def oracle_append(self, spark, df, schema: str, table: str):
        """
        # Oracle 에서 Insert 를 해줄 수 있습니다.
        """

        df.write \
            .format("jdbc") \
            .option("truncate", "true") \
            .option("url", self.configMaster.get_oracle_jdbc_url()) \
            .option("user", self.configMaster.get_oracle_user()) \
            .option("password", self.read_oracle_password(spark)) \
            .option("dbtable", f"{schema}.{table}") \
            .mode('append') \
            .save()
            
    def __del__(self):
        del self.configMaster
