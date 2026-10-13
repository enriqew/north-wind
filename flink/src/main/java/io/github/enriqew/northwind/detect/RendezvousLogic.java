package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.PatternEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Rendezvous: two vessels within {@link #RADIUS_M} of each other, both slower than
 * {@link #MAX_KNOTS}, outside port areas, for at least {@link #MIN_DURATION_MS}.
 *
 * <p>The stream is keyed by grid cell. Each position goes to its own cell as a <em>home</em>
 * visit and to the neighbouring cells as a <em>visitor</em>, so any two vessels within the radius
 * meet in at least one cell. To report a pair once and not in every cell they share, a pair is
 * tracked only in the cell where the vessel with the lower MMSI is at home.
 *
 * <p>Input to {@link #process} must be all visits to one cell at one event time, sorted.
 */
public final class RendezvousLogic implements Serializable {

    public static final double RADIUS_M = 500.0;
    public static final double MAX_KNOTS = 1.0;
    public static final long MIN_DURATION_MS = 30 * 60_000L;
    /** Two reports further apart in time than this are not compared. */
    public static final long PAIRING_WINDOW_MS = 5 * 60_000L;
    /** A pair not seen together for this long has separated. */
    public static final long SEPARATION_MS = 15 * 60_000L;
    public static final double DLAT = Masks.PORT_DLAT;
    public static final double DLON = Masks.PORT_DLON;

    public static class Visit {
        public long cell;
        public boolean home;
        public long mmsi;
        public long t;
        public long offset;
        public double lat;
        public double lon;
        public Integer shipType;
        public Long lastUnderwayMs;

        public Visit() {}
    }

    public static class Pair {
        public long low;
        public long high;
        public Integer lowType;
        public Integer highType;
        public long startT;
        public long lastT;
        public double lat;
        public double lon;
        public double minDistanceM;
        public int points;
        public boolean approached;

        public Pair() {}
    }

    public static class CellState {
        public Map<Long, Visit> latest = new HashMap<>();
        public Map<String, Pair> pairs = new HashMap<>();

        public CellState() {}
    }

    public static boolean eligible(AisRecord r, Masks masks) {
        return r.sog != null
                && r.sog < MAX_KNOTS
                && masks.portAt(r.lat, r.lon) == null
                && !VesselTypes.comesAlongside(r.shipType);
    }

    /** The visits one position produces: its home cell, and every cell within the radius of it. */
    public static List<Visit> route(AisRecord r) {
        int row = Geo.row(r.lat, DLAT);
        int col = Geo.col(r.lon, DLON);
        double widthM = Math.max(1.0, Geo.cellWidthM(r.lat, DLON));
        int dc = (int) Math.ceil(RADIUS_M / widthM);
        List<Visit> visits = new ArrayList<>((2 * dc + 1) * 3);
        for (int i = -1; i <= 1; i++) {
            for (int j = -dc; j <= dc; j++) {
                Visit v = new Visit();
                v.cell = Geo.key(row + i, col + j);
                v.home = i == 0 && j == 0;
                v.mmsi = r.mmsi;
                v.t = r.msgtimeMs;
                v.offset = r.offset;
                v.lat = r.lat;
                v.lon = r.lon;
                v.shipType = r.shipType;
                v.lastUnderwayMs = r.lastUnderwayMs;
                visits.add(v);
            }
        }
        return visits;
    }

    public List<PatternEvent> process(CellState s, List<Visit> visits, long t) {
        for (Visit v : visits) {
            s.latest.put(v.mmsi, v);
        }
        for (Visit v : visits) {
            for (Visit u : s.latest.values()) {
                if (u.mmsi == v.mmsi || Math.abs(u.t - v.t) > PAIRING_WINDOW_MS) {
                    continue;
                }
                Visit low = v.mmsi < u.mmsi ? v : u;
                Visit high = low == v ? u : v;
                if (!low.home) {
                    continue;
                }
                double d = Geo.haversineM(v.lat, v.lon, u.lat, u.lon);
                if (d > RADIUS_M) {
                    continue;
                }
                // Together as of the older of the two reports. Using the current time instead let a
                // vessel that had already left keep a pair alive with its last report from this cell.
                long together = Math.min(v.t, u.t);
                String key = low.mmsi + ":" + high.mmsi;
                Pair p = s.pairs.get(key);
                if (p == null) {
                    p = new Pair();
                    p.low = low.mmsi;
                    p.high = high.mmsi;
                    p.startT = together;
                    p.lat = (low.lat + high.lat) / 2;
                    p.lon = (low.lon + high.lon) / 2;
                    p.minDistanceM = d;
                    p.approached = StopLogic.approached(low.lastUnderwayMs, together)
                            || StopLogic.approached(high.lastUnderwayMs, together);
                    s.pairs.put(key, p);
                }
                p.lowType = low.shipType;
                p.highType = high.shipType;
                p.lastT = Math.max(p.lastT, together);
                p.minDistanceM = Math.min(p.minDistanceM, d);
                p.points++;
            }
        }
        return expire(s, t);
    }

    /** Closes the pairs that have separated by {@code t} and forgets stale positions. */
    public List<PatternEvent> expire(CellState s, long t) {
        List<PatternEvent> out = new ArrayList<>();
        Iterator<Map.Entry<String, Pair>> it = s.pairs.entrySet().iterator();
        while (it.hasNext()) {
            Pair p = it.next().getValue();
            if (t - p.lastT > SEPARATION_MS) {
                PatternEvent e = toEvent(p, false);
                if (e != null) {
                    out.add(e);
                }
                it.remove();
            }
        }
        s.latest.values().removeIf(v -> t - v.t > SEPARATION_MS);
        out.sort((a, b) -> a.startMs != b.startMs ? Long.compare(a.startMs, b.startMs) : Long.compare(a.mmsi, b.mmsi));
        return out;
    }

    /** End of input: the pairs still together are reported as truncated. */
    public List<PatternEvent> flush(CellState s) {
        List<PatternEvent> out = new ArrayList<>();
        for (Pair p : s.pairs.values()) {
            PatternEvent e = toEvent(p, true);
            if (e != null) {
                out.add(e);
            }
        }
        s.pairs.clear();
        s.latest.clear();
        out.sort((a, b) -> a.startMs != b.startMs ? Long.compare(a.startMs, b.startMs) : Long.compare(a.mmsi, b.mmsi));
        return out;
    }

    /** The latest time a pair in this cell could still need closing, so a timer can be set for it. */
    public static long closeTimer(Pair p) {
        return p.lastT + SEPARATION_MS + 1;
    }

    private static PatternEvent toEvent(Pair p, boolean truncated) {
        if (p.lastT - p.startT < MIN_DURATION_MS) {
            return null;
        }
        PatternEvent e = new PatternEvent();
        e.pattern = PatternEvent.RENDEZVOUS;
        e.mmsi = p.low;
        e.shipType = p.lowType;
        e.otherMmsi = p.high;
        e.otherShipType = p.highType;
        e.startMs = p.startT;
        e.endMs = p.lastT;
        e.lat = p.lat;
        e.lon = p.lon;
        e.minDistanceM = p.minDistanceM;
        e.points = p.points;
        e.truncated = truncated;
        e.approached = p.approached;
        return e;
    }
}
