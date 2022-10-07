FROM flink:1.13.6
RUN mkdir -p $FLINK_HOME/app
COPY target/edu-realtime-1.0-SNAPSHOT-jar-with-dependencies.jar $FLINK_HOME/app/my-flink-job.jar