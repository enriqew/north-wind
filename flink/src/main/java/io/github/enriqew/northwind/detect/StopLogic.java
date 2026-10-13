package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.PatternEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Where a vessel stops: port calls inside port areas, loitering outside them.
 *
 * <p><b>Port call.</b> Slower than {@link #PORT_STOP_KNOTS} inside a port area for at least
 * {@link #PORT_DWELL_MS} opens a call, dated from when the vessel first stopped. Manoeuvring inside
 * the port does not end it; the first position outside any port area does.
 *
 * <p><b>Loitering.</b> Slower than {@link #LOITER_KNOTS}, outside every port area, for at least
 * {@link #LOITER_MIN_MS}. A silence longer than {@link #LOITER_MAX_SILENCE_MS} ends the episode,
 * because nothing is known about what the vessel did in it. Vessels whose work is to stop at sea
 * (fishing, tugs, pilots, SAR...) are not considered, see {@link VesselTypes}.
 *
 * <p>Input must be one vessel's positions in event-time order.
 */
public final class StopLogic implements Serializable {

    public static final double PORT_STOP_KNOTS = 0.5;
    public static final long PORT_DWELL_MS = 30 * 60_000L;
    public static final double LOITER_KNOTS = 1.5;
    public static final long LOITER_MIN_MS = 2 * 3_600_000L;
    public static final long LOITER_MAX_SILENCE_MS = 3_600_000L;

    public static class State {
        // Port call
        public Long stopStart;
        public Integer stopPort;
        public Long callStart;
        public Integer callPort;
        public Long callLastT;
        public Double callLat;
        public Double callLon;
        public Integer callPoints;

        // Loitering
        public Long loiterStart;
        public Long loiterLastT;
        public Double loiterLat;
        public Double loiterLon;
        public Double loiterLastLat;
        public Double loiterLastLon;
        public Double loiterMaxDriftM;
        public Integer loiterPoints;
        public Map<Integer, Integer> navStatusCounts;

        public Integer shipType;
        public Integer length;

        public State() {}
    }

    /** Speed for stop decisions: "not available" counts as stopped only when the vessel says it is moored or at anchor. */
    static double effectiveSog(AisRecord r) {
        if (r.sog != null) {
            return r.sog;
        }
        boolean declaredStopped = r.navStatus != null && (r.navStatus == 1 || r.navStatus == 5);
        return declaredStopped ? 0.0 : Double.MAX_VALUE;
    }

    public List<PatternEvent> onPosition(State s, AisRecord r, Masks masks) {
        List<PatternEvent> out = new ArrayList<>(1);
        s.shipType = r.shipType;
        s.length = r.length;
        double sog = effectiveSog(r);
        Integer port = masks.portAt(r.lat, r.lon);

        // Port call
        if (port != null) {
            if (s.callStart != null) {
                s.callLastT = r.msgtimeMs;
                s.callPoints++;
            } else if (sog < PORT_STOP_KNOTS) {
                if (s.stopStart == null || !port.equals(s.stopPort)) {
                    s.stopStart = r.msgtimeMs;
                    s.stopPort = port;
                    s.callLat = r.lat;
                    s.callLon = r.lon;
                }
                if (r.msgtimeMs - s.stopStart >= PORT_DWELL_MS) {
                    s.callStart = s.stopStart;
                    s.callPort = s.stopPort;
                    s.callLastT = r.msgtimeMs;
                    s.callPoints = 1;
                }
            } else {
                s.stopStart = null;
            }
        } else {
            s.stopStart = null;
            if (s.callStart != null) {
                out.add(closeCall(s, r, false));
            }
        }

        // Loitering
        boolean eligible = port == null && sog < LOITER_KNOTS && !VesselTypes.worksAtLowSpeed(r.shipType);
        if (eligible && s.loiterStart != null && r.msgtimeMs - s.loiterLastT > LOITER_MAX_SILENCE_MS) {
            emitLoiter(s, r, false, out);
        }
        if (eligible) {
            if (s.loiterStart == null) {
                s.loiterStart = r.msgtimeMs;
                s.loiterLat = r.lat;
                s.loiterLon = r.lon;
                s.loiterMaxDriftM = 0.0;
                s.loiterPoints = 0;
                s.navStatusCounts = new HashMap<>();
            }
            s.loiterLastT = r.msgtimeMs;
            s.loiterLastLat = r.lat;
            s.loiterLastLon = r.lon;
            s.loiterPoints++;
            s.loiterMaxDriftM = Math.max(s.loiterMaxDriftM, Geo.haversineM(s.loiterLat, s.loiterLon, r.lat, r.lon));
            if (r.navStatus != null) {
                s.navStatusCounts.merge(r.navStatus, 1, Integer::sum);
            }
        } else if (s.loiterStart != null) {
            emitLoiter(s, r, false, out);
        }
        return out;
    }

    /** End of input: report the episodes still open, marked truncated. */
    public List<PatternEvent> flush(State s, long mmsi) {
        List<PatternEvent> out = new ArrayList<>(2);
        if (s.callStart != null) {
            AisRecord last = new AisRecord();
            last.mmsi = mmsi;
            out.add(closeCall(s, last, true));
        }
        if (s.loiterStart != null) {
            AisRecord last = new AisRecord();
            last.mmsi = mmsi;
            emitLoiter(s, last, true, out);
        }
        return out;
    }

    private PatternEvent closeCall(State s, AisRecord r, boolean truncated) {
        PatternEvent e = new PatternEvent();
        e.pattern = PatternEvent.PORT_CALL;
        e.mmsi = r.mmsi;
        e.shipType = s.shipType;
        e.length = s.length;
        e.startMs = s.callStart;
        e.endMs = s.callLastT;
        e.lat = s.callLat;
        e.lon = s.callLon;
        e.portId = s.callPort;
        e.points = s.callPoints;
        e.truncated = truncated;
        s.callStart = null;
        s.callPort = null;
        s.callLastT = null;
        s.callPoints = null;
        return e;
    }

    private void emitLoiter(State s, AisRecord r, boolean truncated, List<PatternEvent> out) {
        if (s.loiterLastT - s.loiterStart >= LOITER_MIN_MS) {
            PatternEvent e = new PatternEvent();
            e.pattern = PatternEvent.LOITERING;
            e.mmsi = r.mmsi;
            e.shipType = s.shipType;
            e.length = s.length;
            e.startMs = s.loiterStart;
            e.endMs = s.loiterLastT;
            e.lat = s.loiterLat;
            e.lon = s.loiterLon;
            e.endLat = s.loiterLastLat;
            e.endLon = s.loiterLastLon;
            e.points = s.loiterPoints;
            e.maxDriftKm = s.loiterMaxDriftM / 1000.0;
            e.navStatusMode = mode(s.navStatusCounts);
            e.truncated = truncated;
            out.add(e);
        }
        s.loiterStart = null;
        s.loiterLastT = null;
        s.navStatusCounts = null;
    }

    private static Integer mode(Map<Integer, Integer> counts) {
        Integer best = null;
        int bestCount = -1;
        if (counts == null) {
            return null;
        }
        for (Map.Entry<Integer, Integer> e : counts.entrySet()) {
            if (e.getValue() > bestCount || (e.getValue() == bestCount && e.getKey() < best)) {
                best = e.getKey();
                bestCount = e.getValue();
            }
        }
        return best;
    }
}
