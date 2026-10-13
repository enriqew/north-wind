package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import io.github.enriqew.northwind.model.AisRecord;
import java.io.Serializable;

/**
 * Rejects positions that would need an impossible speed to reach from the vessel's last accepted
 * one: a corrupted fix, or two transmitters sharing an MMSI.
 *
 * <p>Comparing only against the last accepted position has a failure mode: if that position was
 * the bad one, every good position after it looks impossible. So a rejected position is kept as a
 * candidate, and when the next position agrees with the candidate rather than with the last
 * accepted one, the track moves to it.
 *
 * <p>Input must be one vessel's positions in event-time order.
 */
public final class TrackFilter implements Serializable {

    public static final double MAX_KNOTS = 60.0;
    /** Jumps shorter than this are GPS noise, not teleports, whatever speed they imply. */
    public static final double MIN_JUMP_M = 2_000.0;

    public static class State {
        public Long t;
        public Double lat;
        public Double lon;
        public Long candT;
        public Double candLat;
        public Double candLon;

        public State() {}
    }

    /** True when the position is accepted. Updates the state either way. */
    public boolean accept(State s, AisRecord r) {
        if (s.t == null || plausible(s.t, s.lat, s.lon, r)) {
            take(s, r);
            return true;
        }
        if (s.candT != null && plausible(s.candT, s.candLat, s.candLon, r)) {
            take(s, r);
            return true;
        }
        s.candT = r.msgtimeMs;
        s.candLat = r.lat;
        s.candLon = r.lon;
        return false;
    }

    private static boolean plausible(long t, double lat, double lon, AisRecord r) {
        double meters = Geo.haversineM(lat, lon, r.lat, r.lon);
        return meters < MIN_JUMP_M || Geo.impliedKnots(meters, Math.abs(r.msgtimeMs - t)) <= MAX_KNOTS;
    }

    private static void take(State s, AisRecord r) {
        s.t = r.msgtimeMs;
        s.lat = r.lat;
        s.lon = r.lon;
        s.candT = null;
        s.candLat = null;
        s.candLon = null;
    }
}
