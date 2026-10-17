package io.github.enriqew.northwind.detect;

import static io.github.enriqew.northwind.detect.Fixtures.MIN;
import static io.github.enriqew.northwind.detect.Fixtures.T0;
import static io.github.enriqew.northwind.detect.Fixtures.pos;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.PatternEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class RendezvousLogicTest {

    /**
     * Runs positions through the rendezvous stages the way the job does: heldSince per vessel, visits
     * grouped by cell and processed per event time, then sightings grouped by pair and applied per
     * cell batch time, then flushed.
     */
    static List<PatternEvent> run(List<AisRecord> positions) {
        // Stamp heldSinceMs as VesselCleaner does: per vessel, in event-time order.
        HoldTracker hold = new HoldTracker();
        Map<Long, HoldTracker.State> holds = new HashMap<>();
        List<AisRecord> ordered = new ArrayList<>(positions);
        ordered.sort(Comparator.comparingLong((AisRecord r) -> r.msgtimeMs).thenComparingLong(r -> r.mmsi));
        for (AisRecord r : ordered) {
            r.heldSinceMs = hold.update(holds.computeIfAbsent(r.mmsi, k -> new HoldTracker.State()), r.lat, r.lon, r.msgtimeMs);
        }
        RendezvousLogic logic = new RendezvousLogic();
        Map<Long, TreeMap<Long, List<RendezvousLogic.Visit>>> byCell = new HashMap<>();
        for (AisRecord r : positions) {
            for (RendezvousLogic.Visit v : RendezvousLogic.route(r)) {
                byCell.computeIfAbsent(v.cell, k -> new TreeMap<>())
                        .computeIfAbsent(v.t, k -> new ArrayList<>())
                        .add(v);
            }
        }
        // Cells: each processed per event time in order, producing sightings.
        Map<Long, TreeMap<Long, List<RendezvousLogic.Sighting>>> byPair = new HashMap<>();
        for (TreeMap<Long, List<RendezvousLogic.Visit>> cell : byCell.values()) {
            RendezvousLogic.CellState s = new RendezvousLogic.CellState();
            for (Map.Entry<Long, List<RendezvousLogic.Visit>> e : cell.entrySet()) {
                List<RendezvousLogic.Visit> visits = e.getValue();
                visits.sort(Comparator.comparingLong(v -> v.mmsi));
                for (RendezvousLogic.Sighting g : logic.process(s, visits, e.getKey())) {
                    byPair.computeIfAbsent(g.pairKey(), k -> new TreeMap<>())
                            .computeIfAbsent(g.at, k -> new ArrayList<>())
                            .add(g);
                }
                logic.expire(s, e.getKey());
            }
        }
        long lastT = positions.stream().mapToLong(r -> r.msgtimeMs).max().orElse(0);
        // Pairs: sightings applied per cell batch time, a close check after each, as PairFunction does.
        PairEpisodes episodes = new PairEpisodes();
        List<PatternEvent> out = new ArrayList<>();
        for (Map.Entry<Long, TreeMap<Long, List<RendezvousLogic.Sighting>>> pair : byPair.entrySet()) {
            long lo = pair.getKey() / 1_000_000_000L;
            long hi = pair.getKey() % 1_000_000_000L;
            PairEpisodes.State s = new PairEpisodes.State();
            for (Map.Entry<Long, List<RendezvousLogic.Sighting>> batch : pair.getValue().entrySet()) {
                if (s.startT != null && PairEpisodes.closeTimer(s) < batch.getKey()) {
                    addIfAny(out, episodes.expire(s, PairEpisodes.closeTimer(s), lo, hi));
                }
                List<RendezvousLogic.Sighting> gs = batch.getValue();
                gs.sort(Comparator.comparingLong((RendezvousLogic.Sighting g) -> g.together)
                        .thenComparingDouble(g -> g.distanceM));
                for (RendezvousLogic.Sighting g : gs) {
                    addIfAny(out, episodes.onSighting(s, g));
                }
                addIfAny(out, episodes.expire(s, batch.getKey(), lo, hi));
            }
            // The close timer fires if the watermark reaches it before the input ends.
            if (s.startT != null && PairEpisodes.closeTimer(s) <= lastT) {
                addIfAny(out, episodes.expire(s, PairEpisodes.closeTimer(s), lo, hi));
            }
            addIfAny(out, episodes.flush(s, lo, hi));
        }
        return out;
    }

    private static void addIfAny(List<PatternEvent> out, PatternEvent e) {
        if (e != null) {
            out.add(e);
        }
    }

    @Test
    void twoSlowVesselsSideBySideForAnHourAreOneRendezvous() {
        List<AisRecord> positions = new ArrayList<>();
        for (int m = 0; m <= 60; m += 3) {
            positions.add(pos(100, T0 + m * MIN, 70.10, 20.10, 0.4));
            positions.add(pos(200, T0 + m * MIN, 70.1005, 20.1005, 0.3));
        }
        List<PatternEvent> out = run(positions);
        assertEquals(1, out.size(), "reported once, not once per shared cell");
        PatternEvent e = out.get(0);
        assertEquals(PatternEvent.RENDEZVOUS, e.pattern);
        assertEquals(100, e.mmsi);
        assertEquals(200L, e.otherMmsi);
        assertEquals(60 * 60, e.durationS());
        assertTrue(e.minDistanceM < 100);
        assertTrue(e.truncated);
        assertEquals(false, e.approached, "neither vessel was seen moving: two boats moored together");
        assertEquals(true, e.heldPosition);
    }

    @Test
    void aPairDriftingTogetherDidNotHoldPosition() {
        List<AisRecord> positions = new ArrayList<>();
        for (int m = 0; m <= 120; m += 1) {
            // Both drift north at 0.5 kn, 100 m apart: about 1.9 km over the two hours.
            double north = m * 0.5 * 1852 / 60 / 111_320.0;
            positions.add(pos(100, T0 + m * MIN, 70.10 + north, 20.10, 0.5));
            positions.add(pos(200, T0 + m * MIN, 70.1009 + north, 20.10, 0.5));
        }
        List<PatternEvent> out = run(positions);
        assertEquals(1, out.size(), "one episode across the cells it drifted through, not one per cell");
        assertEquals(120 * 60, out.get(0).durationS());
        assertEquals(false, out.get(0).heldPosition);
    }

    @Test
    void aPairIsApproachedWhenOneVesselArrivedUnderway() {
        List<AisRecord> positions = new ArrayList<>();
        for (int m = 0; m <= 60; m += 3) {
            positions.add(pos(100, T0 + m * MIN, 70.10, 20.10, 0.4));
            AisRecord arriving = pos(200, T0 + m * MIN, 70.1005, 20.1005, 0.3);
            arriving.lastUnderwayMs = T0 - 10 * MIN;
            positions.add(arriving);
        }
        assertEquals(true, run(positions).get(0).approached);
    }

    @Test
    void aPairAcrossACellBorderIsStillFound() {
        // Straddle a row boundary at 70.10 (cells are 0.01 degrees of latitude).
        List<AisRecord> positions = new ArrayList<>();
        for (int m = 0; m <= 45; m += 3) {
            positions.add(pos(300, T0 + m * MIN, 70.0999, 20.10, 0.2));
            positions.add(pos(400, T0 + m * MIN, 70.1001, 20.10, 0.2));
        }
        assertEquals(1, run(positions).size());
    }

    @Test
    void briefOrDistantEncountersAreNot() {
        List<AisRecord> brief = new ArrayList<>();
        for (int m = 0; m <= 20; m += 3) {
            brief.add(pos(1, T0 + m * MIN, 70.10, 20.10, 0.4));
            brief.add(pos(2, T0 + m * MIN, 70.1005, 20.10, 0.4));
        }
        assertTrue(run(brief).isEmpty());

        List<AisRecord> distant = new ArrayList<>();
        for (int m = 0; m <= 60; m += 3) {
            distant.add(pos(1, T0 + m * MIN, 70.10, 20.10, 0.4));
            distant.add(pos(2, T0 + m * MIN, 70.11, 20.10, 0.4)); // 1.1 km apart
        }
        assertTrue(run(distant).isEmpty());
    }

    @Test
    void separationClosesThePair() {
        List<AisRecord> positions = new ArrayList<>();
        for (int m = 0; m <= 40; m += 3) {
            positions.add(pos(1, T0 + m * MIN, 70.10, 20.10, 0.4));
            positions.add(pos(2, T0 + m * MIN, 70.1003, 20.10, 0.4));
        }
        // Vessel 2 stays slow but moves 3 km off; vessel 1 keeps reporting.
        for (int m = 43; m <= 90; m += 3) {
            positions.add(pos(1, T0 + m * MIN, 70.10, 20.10, 0.4));
            positions.add(pos(2, T0 + m * MIN, 70.13, 20.10, 0.4));
        }
        List<PatternEvent> out = run(positions);
        assertEquals(1, out.size());
        assertEquals(false, out.get(0).truncated);
        assertEquals(39 * 60, out.get(0).durationS()); // last report together is at minute 39
    }
}
