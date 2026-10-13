package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.detect.GapLogic;
import io.github.enriqew.northwind.detect.Masks;
import io.github.enriqew.northwind.detect.StopLogic;
import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.PatternEvent;
import java.nio.file.Paths;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** The per-vessel detectors: AIS gaps, port calls and loitering, over one ordered track. */
public class VesselPatterns extends KeyedProcessFunction<Long, AisRecord, PatternEvent> {

    private final String masksDir;
    private final GapLogic gaps;
    private final StopLogic stops = new StopLogic();

    private transient Masks masks;
    private transient ValueState<GapLogic.State> gapState;
    private transient ValueState<StopLogic.State> stopState;

    public VesselPatterns(String masksDir, long gapThresholdMs) {
        this.masksDir = masksDir;
        this.gaps = new GapLogic(gapThresholdMs);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        masks = Masks.load(Paths.get(masksDir));
        gapState = getRuntimeContext().getState(new ValueStateDescriptor<>("gap", Types.POJO(GapLogic.State.class)));
        stopState = getRuntimeContext().getState(new ValueStateDescriptor<>("stop", Types.POJO(StopLogic.State.class)));
    }

    @Override
    public void processElement(AisRecord r, Context ctx, Collector<PatternEvent> out) throws Exception {
        GapLogic.State g = gapState.value();
        StopLogic.State s = stopState.value();
        if (g == null) {
            g = new GapLogic.State();
            s = new StopLogic.State();
            // Fires once, when the input ends, to report what is still open.
            ctx.timerService().registerEventTimeTimer(Long.MAX_VALUE);
        }
        PatternEvent gap = gaps.onPosition(g, r, masks);
        if (gap != null) {
            out.collect(gap);
        }
        for (PatternEvent e : stops.onPosition(s, r, masks)) {
            out.collect(e);
        }
        gapState.update(g);
        stopState.update(s);
    }

    @Override
    public void onTimer(long t, OnTimerContext ctx, Collector<PatternEvent> out) throws Exception {
        StopLogic.State s = stopState.value();
        if (s != null) {
            for (PatternEvent e : stops.flush(s, ctx.getCurrentKey())) {
                out.collect(e);
            }
        }
        gapState.clear();
        stopState.clear();
    }
}
