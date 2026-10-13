package io.github.enriqew.northwind.decode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.RawMessage;
import java.io.Serializable;
import java.time.OffsetDateTime;

/**
 * Turns a BarentsWatch message into an {@link AisRecord}, or says why it cannot.
 *
 * <p>Decoding and validation are one step on purpose: a message is either usable or it carries one
 * reason it is not, and that reason is what the invalid count is broken down by.
 */
public final class AisDecoder implements Serializable {

    /** A message stamped further in the future than this, relative to its own arrival, is not believed. */
    public static final long MAX_FUTURE_MS = 5 * 60_000L;
    /** AIS encodes "speed not available" as 102.3 knots. */
    private static final double SOG_NOT_AVAILABLE = 102.3;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Either a record or a rejection reason, never both. */
    public static final class Result {
        public final AisRecord record;
        public final String reason;
        /** Best-effort fields for the rejection's own accounting. */
        public final String stream;
        public final long msgtimeMs;

        private Result(AisRecord record, String reason, String stream, long msgtimeMs) {
            this.record = record;
            this.reason = reason;
            this.stream = stream;
            this.msgtimeMs = msgtimeMs;
        }

        static Result ok(AisRecord r) {
            return new Result(r, null, r.stream, r.msgtimeMs);
        }

        static Result reject(String reason, String stream, long msgtimeMs) {
            return new Result(null, reason, stream, msgtimeMs);
        }

        public boolean ok() {
            return record != null;
        }
    }

    public Result decode(RawMessage msg) {
        JsonNode n;
        try {
            n = MAPPER.readTree(msg.raw);
        } catch (Exception e) {
            return Result.reject("undecodable", null, 0L);
        }
        if (n == null || !n.isObject()) {
            return Result.reject("undecodable", null, 0L);
        }
        String stream = text(n, "stream");
        long msgtime;
        try {
            msgtime = OffsetDateTime.parse(n.path("msgtime").asText()).toInstant().toEpochMilli();
        } catch (Exception e) {
            return Result.reject("bad_timestamp", stream, 0L);
        }
        if (!n.hasNonNull("mmsi")) {
            return Result.reject("no_mmsi", stream, msgtime);
        }

        AisRecord r = new AisRecord();
        r.mmsi = n.get("mmsi").asLong();
        r.msgtimeMs = msgtime;
        r.arrivalMs = msg.arrivalMs;
        r.offset = msg.offset;
        r.stream = stream;
        r.messageType = n.path("messageType").asInt(0);
        String type = text(n, "type");

        if ("Position".equals(type)) {
            r.kind = AisRecord.POSITION;
            r.aisClass = text(n, "aisClass");
            r.lat = number(n, "latitude");
            r.lon = number(n, "longitude");
            r.sog = number(n, "speedOverGround");
            r.cog = number(n, "courseOverGround");
            r.heading = integer(n, "trueHeading");
            r.navStatus = integer(n, "navigationalStatus");
            if (r.sog != null && r.sog >= SOG_NOT_AVAILABLE) {
                r.sog = null;
            }
        } else if ("Staticdata".equals(type)) {
            r.kind = AisRecord.STATIC;
            r.shipType = integer(n, "shipType");
            r.length = integer(n, "shipLength");
        } else {
            return Result.reject("unsupported_type", stream, msgtime);
        }

        String reason = validate(r);
        return reason == null ? Result.ok(r) : Result.reject(reason, stream, msgtime);
    }

    /** Null when the record is usable, otherwise the first reason it is not. */
    static String validate(AisRecord r) {
        if (r.mmsi <= 0) {
            return "bad_mmsi";
        }
        if (r.msgtimeMs > r.arrivalMs + MAX_FUTURE_MS) {
            return "future_timestamp";
        }
        if (!r.isPosition()) {
            return null;
        }
        if (r.lat == null || r.lon == null) {
            return "no_position";
        }
        if (r.lat == 91.0 || r.lon == 181.0) {
            return "position_not_available";
        }
        if (Math.abs(r.lat) > 90.0 || Math.abs(r.lon) > 180.0) {
            return "out_of_range";
        }
        if (r.lat == 0.0 && r.lon == 0.0) {
            return "null_island";
        }
        return null;
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static Double number(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() || !v.isNumber() ? null : v.asDouble();
    }

    private static Integer integer(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() || !v.isNumber() ? null : v.asInt();
    }
}
