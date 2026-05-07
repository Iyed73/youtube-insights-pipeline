package com.yourpipeline.sentiment;

import com.yourpipeline.model.EnrichedComment;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private transient RobertaSentimentAnalyzer analyzer;

    public SentimentMapFunction(String modelDir) {
        this.modelDir = modelDir;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Initialising RoBERTa sentiment analyzer from {}", modelDir);
        analyzer = new RobertaSentimentAnalyzer(modelDir);
        LOG.info("RoBERTa analyzer ready");
    }

    @Override
    public EnrichedComment map(GenericRecord record) throws Exception {
        String text      = record.get("text").toString();
        String sentiment = analyzer.analyze(text);

        EnrichedComment result = new EnrichedComment();
        result.setCommentId(record.get("comment_id").toString());
        result.setVideoId(record.get("video_id").toString());
        result.setChannelId(record.get("channel_id").toString());
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
