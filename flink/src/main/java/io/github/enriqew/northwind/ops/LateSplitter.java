package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.Disposition;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Separates messages that arrive behind the watermark. They are counted and kept, never dropped
 * silently: on this feed most of them are satellite receptions, and how many there are is a result.
 *
 * <p>Runs at parallelism 1 directly after the watermark assigner, so every message is judged
 * against the watermark produced by exactly the messages before it.
 */
public class LateSplitter extends ProcessFunction<AisRecord, AisRecord> {
    @Override
    public void processElement(AisRecord r, Context ctx, Collector<AisRecord> out) {
        if (r.msgtimeMs <= ctx.timerService().currentWatermark()) {
            Disposition d = Disposition.of(Disposition.LATE, null, r);
            ctx.output(Tags.AUDIT, d);
            ctx.output(Tags.REJECTED, d);
        } else {
            out.collect(r);
        }
    }
}
