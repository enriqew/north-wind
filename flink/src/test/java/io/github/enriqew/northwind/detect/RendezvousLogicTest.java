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
     * Runs positions through routing and per-cell processing the way the job does: visits grouped
     * by cell, each cell processed per event time in order, then flushed.
     */
    static List<PatternEvent> run(List<AisRecord> positions) {
        RendezvousLogic logic = new RendezvousLogic();
        Map<Long, TreeMap<Long, List<RendezvousLogic.Visit>>> byCell = new HashMap<>();
        for (AisRecord r : positions) {
            for (RendezvousLogic.Visit v : RendezvousLogic.route(r)) {
                byCell.computeIfAbsent(v.cell, k -> new TreeMap<>())
                        .computeIfAbsent(v.t, k -> new ArrayList<>())
                        .add(v);
            }
        }
        List<PatternEvent> out = new ArrayList<>();
        for (TreeMap<Long, List<RendezvousLogic.Visit>> cell : byCell.values()) {
            RendezvousLogic.CellState s = new RendezvousLogic.CellState();
            for (Map.Entry<Long, List<RendezvousLogic.Visit>> e : cell.entrySet()) {
                List<RendezvousLogic.Visit> visits = e.getValue();
                visits.sort(Comparator.comparingLong(v -> v.mmsi));
                out.addAll(logic.process(s, visits, e.getKey()));
            }
            out.addAll(logic.flush(s));
        }
        return out;
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
