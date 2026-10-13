package io.github.enriqew.northwind.model;

/**
 * A decoded AIS message: a position report or a static-data report. Public fields and a no-arg
 * constructor keep it a Flink POJO, so it is serialised without Kryo.
 */
public class AisRecord {
    public static final String POSITION = "position";
    public static final String STATIC = "static";

    public String kind;
    public int messageType;
    public long mmsi;
    /** Event time: when the vessel sent the message, as stamped by the receiving station. */
    public long msgtimeMs;
    public long arrivalMs;
    public long offset;
    /** Which receiver network delivered it: terra, offshore or satellite. */
    public String stream;
    public String aisClass;

    public Double lat;
    public Double lon;
    /** Knots; null when the vessel reports "not available". */
    public Double sog;
    public Double cog;
    public Integer heading;
    public Integer navStatus;

    /** From static reports, and copied onto positions once a static report for the vessel is known. */
    public Integer shipType;
    public Integer length;

    /** Last event time this vessel was seen at 1 knot or more, up to and including this position. */
    public Long lastUnderwayMs;

    public AisRecord() {}

    public boolean isPosition() {
        return POSITION.equals(kind);
    }

    /**
     * What makes two receptions the same transmission. The receiving network is deliberately left
     * out: one broadcast heard by a coastal station and a satellite is still one broadcast.
     */
    public String signature() {
        return kind + '|' + messageType + '|' + msgtimeMs + '|' + lat + '|' + lon + '|' + sog + '|' + cog
                + '|' + heading + '|' + navStatus + '|' + shipType + '|' + length;
    }

    public AisRecord copy() {
        AisRecord r = new AisRecord();
        r.kind = kind;
        r.messageType = messageType;
        r.mmsi = mmsi;
        r.msgtimeMs = msgtimeMs;
        r.arrivalMs = arrivalMs;
        r.offset = offset;
        r.stream = stream;
        r.aisClass = aisClass;
        r.lat = lat;
        r.lon = lon;
        r.sog = sog;
        r.cog = cog;
        r.heading = heading;
        r.navStatus = navStatus;
        r.shipType = shipType;
        r.length = length;
        r.lastUnderwayMs = lastUnderwayMs;
        return r;
    }
}
