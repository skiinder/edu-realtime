FROM flink:1.13.6
RUN curl -L https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz | tar zxC /opt
ENV HADOOP_HOME /opt/hadoop-3.3.4
ENV HADOOP_CLASSPATH $HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/yarn:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*
USER flink
RUN mkdir -p $FLINK_HOME/app && \
#    mkdir -p $FLINK_HOME/plugins/juicefs && \
    curl https://repo.maven.apache.org/maven2/io/juicefs/juicefs-hadoop/1.0.0/juicefs-hadoop-1.0.0.jar -o $FLINK_HOME/lib/juicefs-hadoop-1.0.0.jar && \
    curl https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar -o $FLINK_HOME/lib/mysql-connector-java-8.0.30.jar
#    curl https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.2/hadoop-client-api-3.3.2.jar -o $FLINK_HOME/plugins/juicefs/hadoop-client-api-3.3.2.jar && \
#    curl https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.2/hadoop-client-runtime-3.3.2.jar -o $FLINK_HOME/plugins/juicefs/hadoop-client-runtime-3.3.2.jar
COPY app.jar $FLINK_HOME/app
