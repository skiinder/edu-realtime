FROM flink:1.13.6
USER flink
RUN mkdir -p $FLINK_HOME/app && \
    mkdir -p $FLINK_HOME/plugins/juicefs && \
    curl https://repo.maven.apache.org/maven2/io/juicefs/juicefs-hadoop/1.0.0/juicefs-hadoop-1.0.0.jar -o $FLINK_HOME/plugins/juicefs/juicefs-hadoop-1.0.0.jar
#    curl https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.2/hadoop-client-api-3.3.2.jar -o $FLINK_HOME/plugins/juicefs/hadoop-client-api-3.3.2.jar && \
#    curl https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.2/hadoop-client-runtime-3.3.2.jar -o $FLINK_HOME/plugins/juicefs/hadoop-client-runtime-3.3.2.jar
COPY app.jar $FLINK_HOME/app