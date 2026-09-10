package com.yourpipeline.jobs;

import com.yourpipeline.model.EnrichedComment;
import com.yourpipeline.sentiment.SentimentMapFunction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;

public class CommentSentimentJob {

    // Must mirror schemas/raw-comments.avsc.
    private static final String RAW_COMMENT_SCHEMA =
        "{\"type\":\"record\",\"name\":\"RawComment\",\"namespace\":\"com.yourpipeline\"," +
        "\"fields\":[" +
            "{\"name\":\"comment_id\",\"type\":\"string\"}," +
            "{\"name\":\"video_id\",\"type\":\"string\"}," +
            "{\"name\":\"channel_id\",\"type\":\"string\"}," +
            "{\"name\":\"author_channel_id\",\"type\":[\"null\",\"string\"],\"default\":null}," +
            "{\"name\":\"author_display_name\",\"type\":\"string\"}," +
            "{\"name\":\"text\",\"type\":\"string\"}," +
            "{\"name\":\"like_count\",\"type\":\"int\"}," +
            "{\"name\":\"published_at\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
            "{\"name\":\"updated_at\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
            "{\"name\":\"polled_at\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}" +
        "]}";

    private static final String INSERT_SQL =
        "INSERT INTO analytics.comments " +
        "(comment_id, video_id, channel_id, channel_name, author_display_name, author_channel_id, text, sentiment, published_at, processed_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap    = env("KAFKA_BOOTSTRAP_SERVERS",  "kafka:9092");
        String schemaRegistryUrl = env("SCHEMA_REGISTRY_URL",      "http://schema-registry:8080/apis/ccompat/v7");
        String clickhouseUrl     = env("CLICKHOUSE_URL",            "jdbc:clickhouse://clickhouse:8123/analytics");
        String clickhouseUser    = env("CLICKHOUSE_USER",           "admin");
        String clickhousePass    = env("CLICKHOUSE_PASSWORD",       "admin");
        String modelDir          = env("MODEL_DIR",                 "/opt/flink/models/twitter-roberta-sentiment");
        int    parallelism       = Integer.parseInt(env("FLINK_PARALLELISM", "2"));

        String channelsConfig = env("CHANNELS_CONFIG", "/opt/flink/config/channels.yaml");

        Schema readerSchema = new Schema.Parser().parse(RAW_COMMENT_SCHEMA);

        KafkaSource<GenericRecord> kafkaSource = KafkaSource.<GenericRecord>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("raw-comments")
                .setGroupId("flink-sentiment-analyzer")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema
                                .forGeneric(readerSchema, schemaRegistryUrl))
                .build();

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(parallelism);
        streamEnv.enableCheckpointing(30_000);

        DataStream<GenericRecord> comments = streamEnv.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "raw-comments-source");

        DataStream<EnrichedComment> enrichedComments = comments
                .map(new SentimentMapFunction(modelDir, channelsConfig))
                .name("roberta-sentiment");

        enrichedComments.addSink(JdbcSink.sink(
                INSERT_SQL,
                (stmt, r) -> {
                    stmt.setString(1, r.getCommentId());
                    stmt.setString(2, r.getVideoId());
                    stmt.setString(3, r.getChannelId());
                    stmt.setString(4, r.getChannelName());
                    stmt.setString(5, r.getAuthorDisplayName());
                    stmt.setString(6, r.getAuthorChannelId());
                    stmt.setString(7, r.getText());
                    stmt.setString(8, r.getSentiment());
                    stmt.setTimestamp(9, new Timestamp(r.getPublishedAt()));
                    stmt.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(200)
                        .withBatchIntervalMs(2_000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(clickhouseUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .withUsername(clickhouseUser)
                        .withPassword(clickhousePass)
                        .build()
        )).name("clickhouse-sink");

        streamEnv.execute("YouTube Comment Sentiment Analysis");
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : defaultValue;
    }
}
