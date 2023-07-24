from pyspark.sql.types import StructType, StructField, StringType


class SchemaRegistry():

    def kafka_sink_schema(self):
        # Schema 정의
        return StructType([
            StructField("id", StringType()),
            StructField("passwd", StringType())
        ])
