package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import io.github.enriqew.northwind.model.AisRecord;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rendezvous: two vessels within {@link #RADIUS_M} of each other, both slower than
 * {@link #MAX_KNOTS}, outside port areas, for at least {@link #MIN_DURATION_MS}.
 *
 * <p>Finding pairs is keyed by grid cell. Each position goes to its own cell as a <em>home</em>
 * visit and to the neighbouring cells as a <em>visitor</em>, so any two vessels within the radius
 * meet in at least one cell. To report a pair once and not in every cell they share, a cell reports
 * a sighting only when the vessel with the lower MMSI is at home in it.
 *
 * <p>The cells only find pairs. {@link PairEpisodes}, keyed by pair, turns the sightings into
 * episodes, so a pair that drifts from one cell into the next stays one episode. Tracking episodes
 * in the cell, as this did at first, cut a drifting pair into one short episode per cell, each of
 * which looked as if the pair had held position.
 *
 * <p>Every episode carries {@code heldPosition}: both vessels had held position (see
 * {@link HoldTracker}) since before the pair came together, as of the pair's last sighting. Two boats
 * moored side by side at a quay the port mask missed hold position; so do two ships at anchor
 * together, which this cannot tell from a quay without knowing where the shore is. Reported, not
 * filtered.
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
        public Long heldSinceMs;

        public Visit() {}
    }

    /** Two vessels seen within the radius of each other, reported by the low vessel's home cell. */
    public static class Sighting {
        public long low;
        public long high;
        /** Event time of the cell batch that produced it; the pair stage replays sightings in this order. */
        public long at;
        /** The older of the two reports: the pair is known to be together as of then. */
        public long together;
        public double lat;
        public double lon;
        public double distanceM;
        public Integer lowType;
        public Integer highType;
        public Long lowUnderwayMs;
        public Long highUnderwayMs;
        public Long lowHeldSinceMs;
        public Long highHeldSinceMs;

        public Sighting() {}

        /** MMSIs have nine digits, so the pair fits in one long. */
        public long pairKey() {
            return low * 1_000_000_000L + high;
        }
    }

    public static class CellState {
        public Map<Long, Visit> latest = new HashMap<>();

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
                v.heldSinceMs = r.heldSinceMs;
                visits.add(v);
            }
        }
        return visits;
    }

    /**
     * Pairs the visits of one event time with the latest visit of every other vessel in the cell,
     * and returns one sighting per pair within the radius whose low vessel is at home here.
     */
    public List<Sighting> process(CellState s, List<Visit> visits, long t) {
        for (Visit v : visits) {
            s.latest.put(v.mmsi, v);
        }
        List<Sighting> out = new ArrayList<>();
        Set<Long> seen = new HashSet<>();
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
                if (d > RADIUS_M || !seen.add(low.mmsi * 1_000_000_000L + high.mmsi)) {
                    continue;
                }
                Sighting g = new Sighting();
                g.low = low.mmsi;
                g.high = high.mmsi;
                g.at = t;
                // Together as of the older of the two reports. Using the current time instead let a
                // vessel that had already left keep a pair alive with its last report from this cell.
                g.together = Math.min(v.t, u.t);
                g.lat = (low.lat + high.lat) / 2;
                g.lon = (low.lon + high.lon) / 2;
                g.distanceM = d;
                g.lowType = low.shipType;
                g.highType = high.shipType;
                g.lowUnderwayMs = low.lastUnderwayMs;
                g.highUnderwayMs = high.lastUnderwayMs;
                g.lowHeldSinceMs = low.heldSinceMs;
                g.highHeldSinceMs = high.heldSinceMs;
                out.add(g);
            }
        }
        out.sort(Comparator.comparingLong((Sighting g) -> g.low).thenComparingLong(g -> g.high));
        return out;
    }

    /** Forgets positions too old to pair with anything arriving from {@code t} on. */
    public void expire(CellState s, long t) {
        s.latest.values().removeIf(v -> t - v.t > SEPARATION_MS);
    }
}
