package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.model.PatternEvent;
import java.io.Serializable;

/**
 * One pair of vessels over time: turns the sightings the grid cells report into rendezvous
 * episodes. Keyed by the pair rather than by cell, so a pair drifting from one cell into the next is
 * still one episode, and whether it held position is judged over the whole of it.
 *
 * <p>Sightings must be applied in the order the cells emitted them (their {@code at}), each batch
 * followed by {@link #expire}.
 */
public final class PairEpisodes implements Serializable {

    public static class State {
        public Long startT;
        /** The newer report of the first sighting: each vessel's own first report together is at or before it. */
        public long startNewer;
        public long lastT;
        public double lat;
        public double lon;
        public double minDistanceM;
        public int points;
        public boolean approached;
        public Integer lowType;
        public Integer highType;
        public Long lowHeldSinceMs;
        public Long highHeldSinceMs;

        public State() {}
    }

    /** Applies one sighting; returns the episode it closed, if the pair had been apart too long. */
    public PatternEvent onSighting(State s, RendezvousLogic.Sighting g) {
        PatternEvent closed = null;
        if (s.startT != null && g.together - s.lastT > RendezvousLogic.SEPARATION_MS) {
            closed = close(s, g.low, g.high, false);
        }
        if (s.startT == null) {
            s.startT = g.together;
            s.startNewer = g.newer;
            s.lastT = g.together;
            s.lat = g.lat;
            s.lon = g.lon;
            s.minDistanceM = g.distanceM;
            s.points = 0;
            s.approached = StopLogic.approached(g.lowUnderwayMs, g.together)
                    || StopLogic.approached(g.highUnderwayMs, g.together);
        }
        s.lowType = g.lowType;
        s.highType = g.highType;
        s.lowHeldSinceMs = g.lowHeldSinceMs;
        s.highHeldSinceMs = g.highHeldSinceMs;
        s.lastT = Math.max(s.lastT, g.together);
        s.minDistanceM = Math.min(s.minDistanceM, g.distanceM);
        s.points++;
        return closed;
    }

    /** Closes the episode if the pair has not been seen together for longer than the separation. */
    public PatternEvent expire(State s, long t, long low, long high) {
        if (s.startT != null && t - s.lastT > RendezvousLogic.SEPARATION_MS) {
            return close(s, low, high, false);
        }
        return null;
    }

    /** End of input: an episode still open is reported as truncated. */
    public PatternEvent flush(State s, long low, long high) {
        return s.startT == null ? null : close(s, low, high, true);
    }

    public static long closeTimer(State s) {
        return s.lastT + RendezvousLogic.SEPARATION_MS + 1;
    }

    /**
     * Both vessels held position since the pair came together. Compared with the newer report of the
     * first sighting, not with the start: the start is the older report, and a vessel first heard at
     * the beginning of the input is held only since its own first report, a few seconds later.
     */
    static boolean heldPosition(State s) {
        return s.lowHeldSinceMs != null && s.highHeldSinceMs != null
                && Math.max(s.lowHeldSinceMs, s.highHeldSinceMs) <= s.startNewer;
    }

    private static PatternEvent close(State s, long low, long high, boolean truncated) {
        PatternEvent e = null;
        if (s.lastT - s.startT >= RendezvousLogic.MIN_DURATION_MS) {
            e = new PatternEvent();
            e.pattern = PatternEvent.RENDEZVOUS;
            e.mmsi = low;
            e.shipType = s.lowType;
            e.otherMmsi = high;
            e.otherShipType = s.highType;
            e.startMs = s.startT;
            e.endMs = s.lastT;
            e.lat = s.lat;
            e.lon = s.lon;
            e.minDistanceM = s.minDistanceM;
            e.points = s.points;
            e.truncated = truncated;
            e.approached = s.approached;
            e.heldPosition = heldPosition(s);
        }
        s.startT = null;
        return e;
    }
}
