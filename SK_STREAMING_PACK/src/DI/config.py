class ConfigMaster():
    ###################
    # Kafka Sink
    ###################
    __kafka_sink_topic = "topic-01"
    __kafka_sink_cluster = "host1:port host2:port host3:port"
    __kafka_sink_starting_offset = 'latest'

    def get_kafka_sink_topic(self):
        return self.__kafka_sink_topic

    def get_kafka_sink_cluster(self):
        return self.__kafka_sink_cluster

    def get_kafka_sink_start_offset(self):
        return self.__kafka_sink_starting_offset

    ###################
    # Kafka Sink Group ID
    ###################
    __kafka_group_id = "preprocessor-group-01"

    def get_kafka_group_id(self):
        return self.__kafka_group_id

    ###################
    # Oracle
    ###################
    __oracle_jceks = ""
    __oracle_jceks_alias = ""
    __oracle_jdbc_url = ""
    __oracle_user = ""

    def get_oracle_jceks(self):
        return self.__oracle_jceks

    def get_oracle_jceks_alias(self):
        return self.__oracle_jceks_alias

    def get_oracle_jdbc_url(self):
        return self.__oracle_jdbc_url

    def get_oracle_user(self):
        return self.__oracle_user
