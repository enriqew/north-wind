package io.github.enriqew.northwind.detect;

import static io.github.enriqew.northwind.detect.Fixtures.MIN;
import static io.github.enriqew.northwind.detect.Fixtures.T0;
import static io.github.enriqew.northwind.detect.Fixtures.pos;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.PatternEvent;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DetectorsTest {

    // Port cell at Tromsø-ish coordinates; the open sea is ~20 km west.
    static final double PORT_LAT = 69.655;
    static final double PORT_LON = 18.965;
    static final double SEA_LAT = 69.70;
    static final double SEA_LON = 18.40;

    final Masks masks = Fixtures.masksWithPortAt(PORT_LAT, PORT_LON);

    @Test
    void gapIsReportedWithItsContext() {
        GapLogic logic = new GapLogic(60 * MIN);
        GapLogic.State s = new GapLogic.State();
        assertNull(logic.onPosition(s, pos(1, T0, SEA_LAT, SEA_LON, 12.0), masks));
        assertNull(logic.onPosition(s, pos(1, T0 + 30 * MIN, SEA_LAT, SEA_LON + 0.1, 12.0), masks));
        PatternEvent gap = logic.onPosition(s, pos(1, T0 + 120 * MIN, SEA_LAT + 0.2, SEA_LON + 0.2, 12.0), masks);
        assertEquals(PatternEvent.GAP, gap.pattern);
        assertEquals(90 * 60, gap.durationS());
        assertTrue(gap.underway);
        assertTrue(gap.coveredStart);
        assertTrue(gap.coveredEnd);
        assertFalse(gap.outageOverlap);
    }

    @Test
    void gapDuringAnIngestOutageIsFlagged() {
        Masks withOutage = new Masks(new java.util.HashMap<>(), new java.util.HashSet<>(),
                new long[][] {{T0 + 10 * MIN, T0 + 20 * MIN}});
        GapLogic logic = new GapLogic(60 * MIN);
        GapLogic.State s = new GapLogic.State();
        logic.onPosition(s, pos(1, T0, SEA_LAT, SEA_LON, 0.2), withOutage);
        PatternEvent gap = logic.onPosition(s, pos(1, T0 + 70 * MIN, SEA_LAT, SEA_LON, 0.2), withOutage);
        assertTrue(gap.outageOverlap);
        assertFalse(gap.underway);
        assertFalse(gap.coveredStart);
    }

    @Test
    void portCallStartsWhenTheVesselFirstStoppedAndEndsWhenItLeaves() {
        StopLogic logic = new StopLogic();
        StopLogic.State s = new StopLogic.State();
        List<PatternEvent> out = new ArrayList<>();
        out.addAll(logic.onPosition(s, pos(2, T0, PORT_LAT, PORT_LON, 6.0), masks));
        for (int m = 5; m <= 300; m += 5) {
            out.addAll(logic.onPosition(s, pos(2, T0 + m * MIN, PORT_LAT, PORT_LON, 0.0), masks));
        }
        out.addAll(logic.onPosition(s, pos(2, T0 + 310 * MIN, SEA_LAT, SEA_LON, 10.0), masks));
        assertEquals(1, out.size());
        PatternEvent call = out.get(0);
        assertEquals(PatternEvent.PORT_CALL, call.pattern);
        assertEquals(T0 + 5 * MIN, call.startMs);
        assertEquals(T0 + 300 * MIN, call.endMs);
        assertEquals(7, call.portId);
        assertFalse(call.truncated);
    }

    @Test
    void passingThroughAPortIsNotACall() {
        StopLogic logic = new StopLogic();
        StopLogic.State s = new StopLogic.State();
        List<PatternEvent> out = new ArrayList<>();
        for (int m = 0; m <= 20; m += 5) {
            out.addAll(logic.onPosition(s, pos(2, T0 + m * MIN, PORT_LAT, PORT_LON, 0.1), masks));
        }
        out.addAll(logic.onPosition(s, pos(2, T0 + 25 * MIN, SEA_LAT, SEA_LON, 10.0), masks));
        assertTrue(out.isEmpty());
    }

    @Test
    void loiteringNeedsTwoHoursOutsidePort() {
        StopLogic logic = new StopLogic();
        StopLogic.State s = new StopLogic.State();
        List<PatternEvent> out = new ArrayList<>();
        for (int m = 0; m <= 150; m += 10) {
            out.addAll(logic.onPosition(s, pos(3, T0 + m * MIN, SEA_LAT, SEA_LON + m * 1e-4, 0.8), masks));
        }
        out.addAll(logic.onPosition(s, pos(3, T0 + 160 * MIN, SEA_LAT, SEA_LON, 11.0), masks));
        assertEquals(1, out.size());
        PatternEvent e = out.get(0);
        assertEquals(PatternEvent.LOITERING, e.pattern);
        assertEquals(150 * 60, e.durationS());
        assertEquals(16, e.points);
        assertEquals(false, e.approached);
    }

    @Test
    void loiteringRecordsWhetherTheVesselArrivedUnderway() {
        StopLogic logic = new StopLogic();
        StopLogic.State s = new StopLogic.State();
        for (int m = 0; m <= 150; m += 10) {
            AisRecord r = pos(9, T0 + m * MIN, SEA_LAT, SEA_LON, 0.8);
            r.lastUnderwayMs = T0 - 20 * MIN;
            logic.onPosition(s, r, masks);
        }
        assertEquals(true, logic.flush(s, 9).get(0).approached);
    }

    @Test
    void aVesselThatStaysPutHoldsPositionAndADriftingOneDoesNot() {
        StopLogic logic = new StopLogic();
        StopLogic.State moored = new StopLogic.State();
        StopLogic.State drifting = new StopLogic.State();
        for (int m = 0; m <= 150; m += 10) {
            // Moored: GPS noise of a few metres. Drifting: 0.5 kn north, about 2.3 km in 150 minutes.
            logic.onPosition(moored, pos(10, T0 + m * MIN, SEA_LAT + (m % 20 == 0 ? 2e-5 : -2e-5), SEA_LON, 0.1), masks);
            logic.onPosition(drifting, pos(11, T0 + m * MIN, SEA_LAT + m * 0.5 * 1852 / 60 / 111_320.0, SEA_LON, 0.5), masks);
        }
        assertEquals(true, logic.flush(moored, 10).get(0).heldPosition);
        PatternEvent d = logic.flush(drifting, 11).get(0);
        assertEquals(false, d.heldPosition);
        assertTrue(d.maxDriftKm > 2.0);
    }

    @Test
    void oneStrayPositionDoesNotUndoHoldingPosition() {
        StopLogic logic = new StopLogic();
        StopLogic.State s = new StopLogic.State();
        for (int m = 0; m <= 150; m += 10) {
            double lat = m == 70 ? SEA_LAT + 0.015 : SEA_LAT; // one fix 1.7 km off, inside the track filter's tolerance
            logic.onPosition(s, pos(12, T0 + m * MIN, lat, SEA_LON, 0.2), masks);
        }
        assertEquals(true, logic.flush(s, 12).get(0).heldPosition);
    }

    @Test
    void fishingVesselsAreNotLoitering() {
        StopLogic logic = new StopLogic();
        StopLogic.State s = new StopLogic.State();
        List<PatternEvent> out = new ArrayList<>();
        for (int m = 0; m <= 180; m += 10) {
            AisRecord r = pos(4, T0 + m * MIN, SEA_LAT, SEA_LON, 0.5);
            r.shipType = 30;
            out.addAll(logic.onPosition(s, r, masks));
        }
        out.addAll(logic.flush(s, 4));
        assertTrue(out.isEmpty());
    }

    @Test
    void aLongSilenceSplitsLoitering() {
        StopLogic logic = new StopLogic();
        StopLogic.State s = new StopLogic.State();
        List<PatternEvent> out = new ArrayList<>();
        for (int m = 0; m <= 60; m += 10) {
            out.addAll(logic.onPosition(s, pos(5, T0 + m * MIN, SEA_LAT, SEA_LON, 0.3), masks));
        }
        for (int m = 200; m <= 260; m += 10) {
            out.addAll(logic.onPosition(s, pos(5, T0 + m * MIN, SEA_LAT, SEA_LON, 0.3), masks));
        }
        out.addAll(logic.flush(s, 5));
        assertTrue(out.isEmpty(), "two one-hour episodes are not one three-hour episode");
    }

    @Test
    void openLoiteringIsFlushedAsTruncated() {
        StopLogic logic = new StopLogic();
        StopLogic.State s = new StopLogic.State();
        for (int m = 0; m <= 180; m += 10) {
            logic.onPosition(s, pos(6, T0 + m * MIN, SEA_LAT, SEA_LON, 0.3), masks);
        }
        List<PatternEvent> out = logic.flush(s, 6);
        assertEquals(1, out.size());
        assertTrue(out.get(0).truncated);
        assertEquals(6, out.get(0).mmsi);
    }

    @Test
    void trackFilterRejectsATeleportAndRecoversFromABadAnchor() {
        TrackFilter f = new TrackFilter();
        TrackFilter.State s = new TrackFilter.State();
        assertTrue(f.accept(s, pos(7, T0, SEA_LAT, SEA_LON, 10.0)));
        // 1 degree of latitude (111 km) in one minute.
        assertFalse(f.accept(s, pos(7, T0 + MIN, SEA_LAT + 1, SEA_LON, 10.0)));
        assertTrue(f.accept(s, pos(7, T0 + 2 * MIN, SEA_LAT + 0.004, SEA_LON, 10.0)));

        // A bad first fix: the next two good ones agree with each other, so the track moves.
        TrackFilter.State bad = new TrackFilter.State();
        assertTrue(f.accept(bad, pos(8, T0, 0.5, 0.5, 10.0)));
        assertFalse(f.accept(bad, pos(8, T0 + MIN, SEA_LAT, SEA_LON, 10.0)));
        assertTrue(f.accept(bad, pos(8, T0 + 2 * MIN, SEA_LAT + 0.004, SEA_LON, 10.0)));
        assertTrue(f.accept(bad, pos(8, T0 + 3 * MIN, SEA_LAT + 0.008, SEA_LON, 10.0)));
    }
}
