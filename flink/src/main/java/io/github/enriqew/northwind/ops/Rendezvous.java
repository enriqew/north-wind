package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.detect.Masks;
import io.github.enriqew.northwind.detect.RendezvousLogic;
import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.PatternEvent;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Wiring for {@link RendezvousLogic}: route slow positions to grid cells, then pair them per cell. */
public final class Rendezvous {
    private Rendezvous() {}

    /** Slow positions outside port areas, fanned out to their home cell and its neighbours. */
    public static class Router extends RichFlatMapFunction<AisRecord, RendezvousLogic.Visit> {
        private final String masksDir;
        private transient Masks masks;

        public Router(String masksDir) {
            this.masksDir = masksDir;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            masks = Masks.load(Paths.get(masksDir));
        }

        @Override
        public void flatMap(AisRecord r, Collector<RendezvousLogic.Visit> out) {
            if (RendezvousLogic.eligible(r, masks)) {
                for (RendezvousLogic.Visit v : RendezvousLogic.route(r)) {
                    out.collect(v);
                }
            }
        }
    }

    /**
     * One grid cell. Visits come from many upstream subtasks in no particular order, so they are
     * buffered and processed per event time once the watermark passes, sorted by vessel: the
     * result does not depend on which subtask delivered first.
     */
    public static class CellFunction extends KeyedProcessFunction<Long, RendezvousLogic.Visit, PatternEvent> {
        private final RendezvousLogic logic = new RendezvousLogic();

        private transient MapState<Long, List<RendezvousLogic.Visit>> pending;
        private transient ValueState<RendezvousLogic.CellState> cell;

        @Override
        public void open(Configuration parameters) {
            pending = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                    "pending", Types.LONG, Types.LIST(Types.POJO(RendezvousLogic.Visit.class))));
            cell = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "cell", TypeInformation.of(RendezvousLogic.CellState.class)));
        }

        @Override
        public void processElement(RendezvousLogic.Visit v, Context ctx, Collector<PatternEvent> out) throws Exception {
            List<RendezvousLogic.Visit> bucket = pending.get(v.t);
            if (bucket == null) {
                bucket = new ArrayList<>(2);
            }
            bucket.add(v);
            pending.put(v.t, bucket);
            ctx.timerService().registerEventTimeTimer(v.t);
            if (cell.value() == null) {
                cell.update(new RendezvousLogic.CellState());
                ctx.timerService().registerEventTimeTimer(Long.MAX_VALUE);
            }
        }

        @Override
        public void onTimer(long t, OnTimerContext ctx, Collector<PatternEvent> out) throws Exception {
            RendezvousLogic.CellState s = cell.value();
            if (s == null) {
                return;
            }
            // When the input ends, the watermark jumps to the end of time and every pending close
            // timer fires before the flush. Those timers would read "no more data" as "the vessels
            // separated", so at the end only real data is processed and the rest is flushed as
            // truncated.
            boolean inputEnded = ctx.timerService().currentWatermark() == Long.MAX_VALUE;
            List<PatternEvent> events;
            if (t == Long.MAX_VALUE) {
                events = logic.flush(s);
                cell.clear();
            } else if (inputEnded && !pending.contains(t)) {
                return;
            } else {
                List<RendezvousLogic.Visit> bucket = pending.get(t);
                if (bucket != null) {
                    pending.remove(t);
                    bucket.sort(Comparator.<RendezvousLogic.Visit>comparingLong(v -> v.mmsi)
                            .thenComparingLong(v -> v.offset));
                    events = logic.process(s, bucket, t);
                    for (RendezvousLogic.Pair p : s.pairs.values()) {
                        ctx.timerService().registerEventTimeTimer(RendezvousLogic.closeTimer(p));
                    }
                } else {
                    events = logic.expire(s, t);
                }
                cell.update(s);
            }
            for (PatternEvent e : events) {
                out.collect(e);
            }
        }
    }
}
