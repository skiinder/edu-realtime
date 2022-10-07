FROM flink:1.13.6
RUN curl -L https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz | tar zxC /opt
ENV HADOOP_HOME /opt/hadoop-3.3.4
ENV HADOOP_CLASSPATH $(/opt/hadoop-3.3.4/bin/hadoop classpath)
USER flink
RUN mkdir -p $FLINK_HOME/app && \
    mkdir -p $FLINK_HOME/plugins/juicefs && \
    curl https://repo.maven.apache.org/maven2/io/juicefs/juicefs-hadoop/1.0.0/juicefs-hadoop-1.0.0.jar -o $FLINK_HOME/plugins/juicefs/juicefs-hadoop-1.0.0.jar
#    curl https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.2/hadoop-client-api-3.3.2.jar -o $FLINK_HOME/plugins/juicefs/hadoop-client-api-3.3.2.jar && \
#    curl https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.2/hadoop-client-runtime-3.3.2.jar -o $FLINK_HOME/plugins/juicefs/hadoop-client-runtime-3.3.2.jar
COPY app.jar $FLINK_HOME/app