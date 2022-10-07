FROM flink:1.13.6
USER flink
RUN mkdir -p $FLINK_HOME/app && \
    mkdir -p $FLINK_HOME/plugins/juicefs && \
    curl https://repo.maven.apache.org/maven2/io/juicefs/juicefs-hadoop/1.0.0/juicefs-hadoop-1.0.0.jar -o $FLINK_HOME/plugins/juicefs/juicefs-hadoop-1.0.0.jar
COPY app.jar $FLINK_HOME/app