package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import java.io.Serializable;

/**
 * Since when a vessel has held position: stayed within {@link StopLogic#HOLD_RADIUS_M} of one point.
 *
 * <p>The point is the vessel's anchor. A position further than the radius from it becomes a
 * candidate; if the next position is also further, the vessel has moved, and the candidate becomes
 * the new anchor, held since the candidate's time. One stray fix (the track filter lets jumps under
 * 2 km through) therefore does not reset it. A moored or anchored vessel keeps one anchor for as long
 * as it stays; a vessel drifting or under way gets a new one every kilometre or so.
 *
 * <p>Input must be one vessel's clean positions in event-time order.
 */
public final class HoldTracker implements Serializable {

    public static class State {
        public Double anchorLat;
        public Double anchorLon;
        public Long sinceMs;
        public Double candLat;
        public Double candLon;
        public Long candMs;

        public State() {}
    }

    /** Takes one position and returns the time since which the vessel has held position. */
    public long update(State s, double lat, double lon, long t) {
        if (s.anchorLat == null) {
            anchor(s, lat, lon, t);
            return s.sinceMs;
        }
        if (Geo.haversineM(s.anchorLat, s.anchorLon, lat, lon) <= StopLogic.HOLD_RADIUS_M) {
            s.candLat = null;
            return s.sinceMs;
        }
        if (s.candLat == null) {
            s.candLat = lat;
            s.candLon = lon;
            s.candMs = t;
            return s.sinceMs;
        }
        anchor(s, s.candLat, s.candLon, s.candMs);
        if (Geo.haversineM(s.anchorLat, s.anchorLon, lat, lon) > StopLogic.HOLD_RADIUS_M) {
            s.candLat = lat;
            s.candLon = lon;
            s.candMs = t;
        }
        return s.sinceMs;
    }

    private static void anchor(State s, double lat, double lon, long t) {
        s.anchorLat = lat;
        s.anchorLon = lon;
        s.sinceMs = t;
        s.candLat = null;
        s.candLon = null;
        s.candMs = null;
    }
}
