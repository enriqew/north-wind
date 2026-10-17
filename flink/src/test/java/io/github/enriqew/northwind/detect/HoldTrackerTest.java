package io.github.enriqew.northwind.detect;

import static io.github.enriqew.northwind.detect.Fixtures.MIN;
import static io.github.enriqew.northwind.detect.Fixtures.T0;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HoldTrackerTest {

    private static final double LAT = 69.70;
    private static final double LON = 19.00;
    /** Degrees of latitude per metre. */
    private static final double DEG_PER_M = 1 / 111_320.0;

    @Test
    void aMooredVesselHoldsPositionFromItsFirstReport() {
        HoldTracker h = new HoldTracker();
        HoldTracker.State s = new HoldTracker.State();
        long since = 0;
        for (int m = 0; m <= 180; m += 5) {
            since = h.update(s, LAT + (m % 10 == 0 ? 10 : -10) * DEG_PER_M, LON, T0 + m * MIN);
        }
        assertEquals(T0, since);
    }

    @Test
    void oneStrayFixDoesNotResetIt() {
        HoldTracker h = new HoldTracker();
        HoldTracker.State s = new HoldTracker.State();
        long since = 0;
        for (int m = 0; m <= 180; m += 5) {
            double lat = m == 90 ? LAT + 1_700 * DEG_PER_M : LAT;
            since = h.update(s, lat, LON, T0 + m * MIN);
        }
        assertEquals(T0, since);
    }

    @Test
    void aDriftingVesselKeepsMovingItsAnchor() {
        HoldTracker h = new HoldTracker();
        HoldTracker.State s = new HoldTracker.State();
        long since = 0;
        for (int m = 0; m <= 180; m += 1) {
            // 0.5 kn north: about 15 m a minute, 2.8 km in three hours.
            since = h.update(s, LAT + m * 0.5 * 1852 / 60 * DEG_PER_M, LON, T0 + m * MIN);
        }
        assertTrue(since > T0 + 120 * MIN, "held since " + (since - T0) / MIN + " min, expected the last hour");
    }
}
