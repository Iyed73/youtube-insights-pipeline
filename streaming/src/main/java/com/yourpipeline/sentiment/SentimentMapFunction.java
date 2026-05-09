package com.yourpipeline.sentiment;

import com.yourpipeline.model.EnrichedComment;
import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink RichMapFunction that applies RoBERTa sentiment analysis to each
 * incoming raw comment.
 *
 * The analyzer is initialised once per TaskManager slot in {@code open()} so
 * the ONNX session and HuggingFace tokenizer are not re-created per record.
 * The field is {@code transient} because {@link RobertaSentimentAnalyzer}
 * holds native ONNX Runtime resources that are not Java-serialisable.
 */
public class SentimentMapFunction extends RichMapFunction<GenericRecord, EnrichedComment> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SentimentMapFunction.class);

    private final String modelDir;
    private final String channelsConfigPath;
    private transient RobertaSentimentAnalyzer analyzer;
    private transient Map<String, String> channelNames;
    private transient Histogram inferenceTimeHistogram;
    private transient Histogram kafkaToFlinkLatencyHistogram;

    public SentimentMapFunction(String modelDir, String channelsConfigPath) {
        this.modelDir = modelDir;
        this.channelsConfigPath = channelsConfigPath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Initialising RoBERTa sentiment analyzer from {}", modelDir);
        analyzer = new RobertaSentimentAnalyzer(modelDir);
        LOG.info("RoBERTa analyzer ready");

        channelNames = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(channelsConfigPath))) {
            String line;
            String currentId = null;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.startsWith("#") || trimmed.isEmpty()) continue;
                if (trimmed.startsWith("- id:")) {
                    currentId = trimmed.substring("- id:".length()).trim();
                } else if (trimmed.startsWith("name:") && currentId != null) {
                    channelNames.put(currentId, trimmed.substring("name:".length()).trim());
                    currentId = null;
                }
            }
        }
        LOG.info("Loaded {} channel name(s) from {}", channelNames.size(), channelsConfigPath);

        inferenceTimeHistogram = getRuntimeContext().getMetricGroup()
                .histogram("inferenceTimeMs", new DropwizardHistogramWrapper(
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(1000))));

        kafkaToFlinkLatencyHistogram = getRuntimeContext().getMetricGroup()
                .histogram("kafkaToFlinkLatencyMs", new DropwizardHistogramWrapper(
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(1000))));
    }

    @Override
    public EnrichedComment map(GenericRecord record) throws Exception {
        String text = record.get("text").toString();

        long inferenceStart = System.currentTimeMillis();
        String sentiment = analyzer.analyze(text);
        inferenceTimeHistogram.update(System.currentTimeMillis() - inferenceStart);

        long polledAt = (Long) record.get("polled_at");
        kafkaToFlinkLatencyHistogram.update(System.currentTimeMillis() - polledAt);

        EnrichedComment result = new EnrichedComment();
        result.setCommentId(record.get("comment_id").toString());
        result.setVideoId(record.get("video_id").toString());
        result.setChannelId(record.get("channel_id").toString());
        result.setChannelName(channelNames.getOrDefault(result.getChannelId(), result.getChannelId()));
        result.setAuthorDisplayName(record.get("author_display_name").toString());
        Object authorChannelId = record.get("author_channel_id");
        result.setAuthorChannelId(authorChannelId != null ? authorChannelId.toString() : null);
        result.setText(text);
        result.setSentiment(sentiment);
        result.setPublishedAt((Long) record.get("published_at"));

        LOG.debug("comment={} sentiment={}", result.getCommentId(), sentiment);
        return result;
    }

    @Override
    public void close() throws Exception {
        if (analyzer != null) {
            analyzer.close();
        }
    }
}
