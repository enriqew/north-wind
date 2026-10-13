package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.model.AisRecord;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * Bounded out-of-orderness, emitted on every event instead of on a timer.
 *
 * <p>Flink's built-in bounded generator emits periodically, on processing time. Which messages
 * then count as late depends on how fast the machine replays them, so two runs over the same
 * recording disagree. Emitting after each event ties the watermark to the input alone: message
 * {@code n} is judged against the largest event time among messages {@code 0..n-1}, on any
 * machine, at any speed.
 */
public class PunctuatedBoundedWatermarks implements WatermarkGenerator<AisRecord> {
    private final long boundMs;
    private long maxTimestamp = Long.MIN_VALUE;

    public PunctuatedBoundedWatermarks(long boundMs) {
        this.boundMs = boundMs;
    }

    @Override
    public void onEvent(AisRecord event, long eventTimestamp, WatermarkOutput output) {
        if (eventTimestamp > maxTimestamp) {
            maxTimestamp = eventTimestamp;
            output.emitWatermark(new Watermark(maxTimestamp - boundMs - 1));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // Deliberately nothing: see the class comment.
    }
}
