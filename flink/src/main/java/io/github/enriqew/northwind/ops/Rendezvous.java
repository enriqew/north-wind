package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.detect.Masks;
import io.github.enriqew.northwind.detect.PairEpisodes;
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

/**
 * Wiring for rendezvous: route slow positions to grid cells, find pairs per cell
 * ({@link RendezvousLogic}), then follow each pair over time ({@link PairEpisodes}).
 */
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
    public static class CellFunction
            extends KeyedProcessFunction<Long, RendezvousLogic.Visit, RendezvousLogic.Sighting> {
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
        public void processElement(RendezvousLogic.Visit v, Context ctx, Collector<RendezvousLogic.Sighting> out)
                throws Exception {
            List<RendezvousLogic.Visit> bucket = pending.get(v.t);
            if (bucket == null) {
                bucket = new ArrayList<>(2);
            }
            bucket.add(v);
            pending.put(v.t, bucket);
            ctx.timerService().registerEventTimeTimer(v.t);
        }

        @Override
        public void onTimer(long t, OnTimerContext ctx, Collector<RendezvousLogic.Sighting> out) throws Exception {
            List<RendezvousLogic.Visit> bucket = pending.get(t);
            if (bucket == null) {
                return;
            }
            pending.remove(t);
            RendezvousLogic.CellState s = cell.value();
            if (s == null) {
                s = new RendezvousLogic.CellState();
            }
            bucket.sort(Comparator.<RendezvousLogic.Visit>comparingLong(v -> v.mmsi).thenComparingLong(v -> v.offset));
            for (RendezvousLogic.Sighting g : logic.process(s, bucket, t)) {
                out.collect(g);
            }
            logic.expire(s, t);
            cell.update(s);
        }
    }

    /**
     * One pair of vessels. Sightings of a pair can come from several cells, so from several
     * subtasks; they are buffered by the event time of the cell batch that produced them and
     * applied in that order, sorted within a batch, once the watermark passes it.
     */
    public static class PairFunction
            extends KeyedProcessFunction<Long, RendezvousLogic.Sighting, PatternEvent> {
        private final PairEpisodes logic = new PairEpisodes();

        private transient MapState<Long, List<RendezvousLogic.Sighting>> pending;
        private transient ValueState<PairEpisodes.State> pair;
        private transient ValueState<Long> low;
        private transient ValueState<Long> high;

        @Override
        public void open(Configuration parameters) {
            pending = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                    "pending", Types.LONG, Types.LIST(Types.POJO(RendezvousLogic.Sighting.class))));
            pair = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "pair", Types.POJO(PairEpisodes.State.class)));
            low = getRuntimeContext().getState(new ValueStateDescriptor<>("low", Types.LONG));
            high = getRuntimeContext().getState(new ValueStateDescriptor<>("high", Types.LONG));
        }

        @Override
        public void processElement(RendezvousLogic.Sighting g, Context ctx, Collector<PatternEvent> out)
                throws Exception {
            List<RendezvousLogic.Sighting> bucket = pending.get(g.at);
            if (bucket == null) {
                bucket = new ArrayList<>(1);
            }
            bucket.add(g);
            pending.put(g.at, bucket);
            ctx.timerService().registerEventTimeTimer(g.at);
            if (pair.value() == null) {
                pair.update(new PairEpisodes.State());
                low.update(g.low);
                high.update(g.high);
                ctx.timerService().registerEventTimeTimer(Long.MAX_VALUE);
            }
        }

        @Override
        public void onTimer(long t, OnTimerContext ctx, Collector<PatternEvent> out) throws Exception {
            PairEpisodes.State s = pair.value();
            if (s == null) {
                return;
            }
            long lo = low.value();
            long hi = high.value();
            // When the input ends the watermark jumps to the end of time and every pending close
            // timer fires before the flush. Those timers would read "no more data" as "the vessels
            // separated", so at the end only real sightings are applied and the rest is flushed as
            // truncated.
            boolean inputEnded = ctx.timerService().currentWatermark() == Long.MAX_VALUE;
            List<PatternEvent> events = new ArrayList<>(1);
            if (t == Long.MAX_VALUE) {
                add(events, logic.flush(s, lo, hi));
                pair.clear();
                low.clear();
                high.clear();
            } else if (inputEnded && !pending.contains(t)) {
                return;
            } else {
                List<RendezvousLogic.Sighting> bucket = pending.get(t);
                if (bucket != null) {
                    pending.remove(t);
                    bucket.sort(Comparator.<RendezvousLogic.Sighting>comparingLong(g -> g.together)
                            .thenComparingDouble(g -> g.distanceM)
                            .thenComparingDouble(g -> g.lat)
                            .thenComparingDouble(g -> g.lon));
                    for (RendezvousLogic.Sighting g : bucket) {
                        add(events, logic.onSighting(s, g));
                    }
                }
                add(events, logic.expire(s, t, lo, hi));
                if (s.startT != null) {
                    ctx.timerService().registerEventTimeTimer(PairEpisodes.closeTimer(s));
                }
                pair.update(s);
            }
            for (PatternEvent e : events) {
                out.collect(e);
            }
        }

        private static void add(List<PatternEvent> events, PatternEvent e) {
            if (e != null) {
                events.add(e);
            }
        }
    }
}
