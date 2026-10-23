package io.github.enriqew.northwind.model;

/** One detection. Which fields are set depends on the pattern; the rest stay null. */
public class PatternEvent {
    public static final String GAP = "ais_gap";
    public static final String PORT_CALL = "port_call";
    public static final String LOITERING = "loitering";
    public static final String RENDEZVOUS = "rendezvous";

    public String pattern;
    public long mmsi;
    public Integer shipType;
    public Integer length;
    public long startMs;
    public long endMs;
    public double lat;
    public double lon;
    public Double endLat;
    public Double endLon;
    /** Positions that make up the episode. */
    public Integer points;
    /** The episode was still open when the input ended, so its end is the end of the recording. */
    public boolean truncated;
    /**
     * Loitering and rendezvous: a vessel involved was moving (1 kn or more) within the hour before
     * the episode began. Without it, the episode may simply be a vessel that was already stopped,
     * or two moored side by side, when the window opened.
     */
    public Boolean approached;

    // ais_gap
    public Double distanceKm;
    public Double impliedKnots;
    public Boolean underway;
    public Boolean coveredStart;
    public Boolean coveredEnd;
    /** The ingest itself was disconnected during the gap, so the silence may be ours. */
    public Boolean outageOverlap;

    // port_call
    public Integer portId;

    // loitering
    public Integer navStatusMode;
    public Double maxDriftKm;
    /**
     * Loitering: stayed within a kilometre of where it began. Rendezvous: both vessels had held
     * position since before they came together. Either way, moored or at anchor.
     */
    public Boolean heldPosition;
    /**
     * Loitering and rendezvous: where the episode began relative to the shore, quay, coast, near or
     * open (see {@code Shore}). Holding position at the shore is moored; in open water, at anchor.
     */
    public String shore;
    /** Metres from where the episode began to the nearest coast, built or natural; null beyond a cell. */
    public Double shoreM;

    // rendezvous
    public Long otherMmsi;
    public Integer otherShipType;
    public Double minDistanceM;

    public PatternEvent() {}

    public long durationS() {
        return (endMs - startMs) / 1000;
    }
}
