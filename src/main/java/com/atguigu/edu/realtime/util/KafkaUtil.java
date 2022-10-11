package com.atguigu.edu.realtime.util;

import com.atguigu.edu.realtime.common.EduConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * description:
 * Created by 铁盾 on 2022/6/13
 */
public class KafkaUtil {
    static final String BOOTSTRAP_SERVERS = EduConfig.KAFKA_BOOTSTRAP_SERVER;
    static final String DEFAULT_TOPIC = "default_topic";

    static Properties consumerProps = new Properties();
    static Properties producerProps = new Properties();

    static {
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Long.toString(15 * 60 * 1000L));
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
        consumerProps.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        return new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                if (consumerRecord != null && consumerRecord.value() != null) {
                    return new String(consumerRecord.value());
                }
                return null;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, consumerProps);
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

        return new FlinkKafkaProducer<String>(DEFAULT_TOPIC, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long aLong) {
                if (jsonStr != null) {
                    return new ProducerRecord<byte[], byte[]>(topic, jsonStr.getBytes());
                }
                return null;
            }
        }, producerProps, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets')";
    }

    /**
     * UpsertKafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 UpsertKafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {

        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }


    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getKafkaSinkDDL(String topic) {
        return "WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'format' = 'json' " +
                ")";
    }

    public static void createTopicDb(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`ts` string\n" +
                ")" + getKafkaDDL("topic_db", groupId));
    }
}
