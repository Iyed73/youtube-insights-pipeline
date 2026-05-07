package com.yourpipeline.model;

import java.io.Serializable;

/**
 * A raw comment enriched with a RoBERTa sentiment label.
 * Written to the ClickHouse analytics.comments table via the Flink JDBC sink.
 */
public class EnrichedComment implements Serializable {
    private static final long serialVersionUID = 1L;

    private String commentId;
    private String videoId;
    private String channelId;
    private String authorDisplayName;
    /** Nullable — null if the commenter has no public YouTube channel */
    private String authorChannelId;
    private String text;
    /** "Positive", "Neutral", or "Negative" */
    private String sentiment;
    /** UTC epoch-millis from the raw comment's published_at field */
    private long   publishedAt;

    public EnrichedComment() {}

    public String getCommentId()            { return commentId; }
    public void   setCommentId(String v)    { this.commentId = v; }

    public String getVideoId()              { return videoId; }
    public void   setVideoId(String v)      { this.videoId = v; }

    public String getChannelId()            { return channelId; }
    public void   setChannelId(String v)    { this.channelId = v; }

    public String getAuthorDisplayName()           { return authorDisplayName; }
    public void   setAuthorDisplayName(String v)   { this.authorDisplayName = v; }

    public String getAuthorChannelId()             { return authorChannelId; }
    public void   setAuthorChannelId(String v)     { this.authorChannelId = v; }

    public String getText()                 { return text; }
    public void   setText(String v)         { this.text = v; }

    public String getSentiment()            { return sentiment; }
    public void   setSentiment(String v)    { this.sentiment = v; }

    public long   getPublishedAt()          { return publishedAt; }
    public void   setPublishedAt(long v)    { this.publishedAt = v; }

    @Override
    public String toString() {
        return "EnrichedComment{" +
               "commentId='" + commentId + '\'' +
               ", videoId='" + videoId + '\'' +
               ", channelId='" + channelId + '\'' +
               ", sentiment='" + sentiment + '\'' +
               ", publishedAt=" + publishedAt +
               '}';
    }
}
