package io.github.enriqew.northwind.model;

/** One record of the replayed topic: the message bytes as received, and where and when it arrived. */
public class RawMessage {
    /** Offset in the single-partition replay topic: the order of arrival, and a stable tie-breaker. */
    public long offset;
    /** Wall-clock time the ingest received the message (the Kafka record timestamp). */
    public long arrivalMs;
    public String raw;

    public RawMessage() {}

    public RawMessage(long offset, long arrivalMs, String raw) {
        this.offset = offset;
        this.arrivalMs = arrivalMs;
        this.raw = raw;
    }
}
