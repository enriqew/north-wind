package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.decode.AisDecoder;
import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.Disposition;
import io.github.enriqew.northwind.model.RawMessage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Decodes and validates. Counts every message as input, and every rejection as invalid with its reason. */
public class DecodeFunction extends ProcessFunction<RawMessage, AisRecord> {
    private final AisDecoder decoder = new AisDecoder();
    private final long windowStartMs;
    private final long windowEndMs;

    public DecodeFunction(long windowStartMs, long windowEndMs) {
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
    }

    @Override
    public void processElement(RawMessage msg, Context ctx, Collector<AisRecord> out) {
        AisDecoder.Result result = decoder.decode(msg);
        ctx.output(Tags.AUDIT, new Disposition(Disposition.INPUT, null, result.stream, result.msgtimeMs, msg.arrivalMs));
        if (result.ok() && (result.msgtimeMs < windowStartMs || result.msgtimeMs >= windowEndMs)) {
            ctx.output(Tags.AUDIT, new Disposition(
                    Disposition.OUTSIDE_WINDOW, null, result.stream, result.msgtimeMs, msg.arrivalMs));
        } else if (result.ok()) {
            out.collect(result.record);
        } else {
            Disposition d = new Disposition(Disposition.INVALID, result.reason, result.stream, result.msgtimeMs, msg.arrivalMs);
            ctx.output(Tags.AUDIT, d);
            ctx.output(Tags.REJECTED, d);
        }
    }
}
