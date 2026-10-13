package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.PatternEvent;
import java.io.Serializable;

/**
 * AIS gaps: a vessel that stops reporting for longer than the threshold and then reappears.
 *
 * <p>Every silence over the threshold is emitted, with the facts needed to judge it: was the vessel
 * underway when it went quiet, were both ends inside continuous coverage, and was the ingest itself
 * disconnected at the time. Deciding which silences count is the export's job, so the funnel from
 * "every silence" to "silences that are hard to explain" stays visible instead of being hidden in a
 * filter. A vessel that goes quiet and never comes back within the recording is not reported: there
 * is no second end to measure.
 *
 * <p>Input must be one vessel's positions in event-time order.
 */
public final class GapLogic implements Serializable {

    public static final double UNDERWAY_KNOTS = 1.0;

    public static class State {
        public Long t;
        public Double lat;
        public Double lon;
        public Double sog;

        public State() {}
    }

    private final long thresholdMs;

    public GapLogic(long thresholdMs) {
        this.thresholdMs = thresholdMs;
    }

    /** Updates the state and returns the gap that this position closes, if any. */
    public PatternEvent onPosition(State s, AisRecord r, Masks masks) {
        PatternEvent gap = null;
        if (s.t != null && r.msgtimeMs - s.t >= thresholdMs) {
            double meters = Geo.haversineM(s.lat, s.lon, r.lat, r.lon);
            gap = new PatternEvent();
            gap.pattern = PatternEvent.GAP;
            gap.mmsi = r.mmsi;
            gap.shipType = r.shipType;
            gap.length = r.length;
            gap.startMs = s.t;
            gap.endMs = r.msgtimeMs;
            gap.lat = s.lat;
            gap.lon = s.lon;
            gap.endLat = r.lat;
            gap.endLon = r.lon;
            gap.points = 2;
            gap.distanceKm = meters / 1000.0;
            gap.impliedKnots = Geo.impliedKnots(meters, r.msgtimeMs - s.t);
            gap.underway = s.sog != null && s.sog >= UNDERWAY_KNOTS;
            gap.coveredStart = masks.covered(s.lat, s.lon);
            gap.coveredEnd = masks.covered(r.lat, r.lon);
            gap.outageOverlap = masks.outageOverlaps(s.t, r.msgtimeMs);
        }
        s.t = r.msgtimeMs;
        s.lat = r.lat;
        s.lon = r.lon;
        s.sog = r.sog;
        return gap;
    }
}
