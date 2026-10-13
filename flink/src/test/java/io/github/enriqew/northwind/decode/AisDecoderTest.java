package io.github.enriqew.northwind.decode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.enriqew.northwind.model.AisRecord;
import io.github.enriqew.northwind.model.RawMessage;
import org.junit.jupiter.api.Test;

class AisDecoderTest {
    static final long ARRIVAL = 1_791_884_220_000L; // 2026-10-13T09:37:00Z
    final AisDecoder decoder = new AisDecoder();

    static RawMessage raw(String json) {
        return new RawMessage(42, ARRIVAL, json);
    }

    static String position(String lat, String lon, String sog, String msgtime) {
        return "{\"type\":\"Position\",\"messageType\":1,\"courseOverGround\":12.5,\"aisClass\":\"A\","
                + "\"latitude\":" + lat + ",\"longitude\":" + lon + ",\"navigationalStatus\":0,"
                + "\"rateOfTurn\":null,\"speedOverGround\":" + sog + ",\"trueHeading\":null,"
                + "\"mmsi\":257668800,\"msgtime\":\"" + msgtime + "\",\"stream\":\"terra\"}";
    }

    @Test
    void decodesAPosition() {
        AisDecoder.Result r = decoder.decode(raw(position("69.508995", "17.908903", "8.4", "2026-10-13T09:36:56+00:00")));
        assertTrue(r.ok());
        AisRecord a = r.record;
        assertEquals(AisRecord.POSITION, a.kind);
        assertEquals(257668800L, a.mmsi);
        assertEquals(ARRIVAL - 4_000, a.msgtimeMs);
        assertEquals(69.508995, a.lat);
        assertEquals(8.4, a.sog);
        assertNull(a.heading);
        assertEquals("terra", a.stream);
        assertEquals(42, a.offset);
    }

    @Test
    void speedNotAvailableBecomesNull() {
        assertNull(decoder.decode(raw(position("69.5", "17.9", "102.3", "2026-10-13T09:36:56+00:00"))).record.sog);
    }

    @Test
    void decodesStaticData() {
        String json = "{\"type\":\"Staticdata\",\"messageType\":5,\"mmsi\":257088000,"
                + "\"msgtime\":\"2026-10-13T09:36:50+00:00\",\"name\":\"X\",\"shipType\":60,"
                + "\"shipLength\":123,\"stream\":\"satellite\"}";
        AisDecoder.Result r = decoder.decode(raw(json));
        assertTrue(r.ok());
        assertEquals(AisRecord.STATIC, r.record.kind);
        assertEquals(60, r.record.shipType);
        assertEquals(123, r.record.length);
    }

    @Test
    void rejectsWithAReason() {
        assertEquals("undecodable", decoder.decode(raw("{\"type\":\"Posit")).reason);
        assertEquals("position_not_available",
                decoder.decode(raw(position("91", "181", "0", "2026-10-13T09:36:56+00:00"))).reason);
        assertEquals("null_island", decoder.decode(raw(position("0", "0", "0", "2026-10-13T09:36:56+00:00"))).reason);
        assertEquals("no_position", decoder.decode(raw(position("null", "null", "0", "2026-10-13T09:36:56+00:00"))).reason);
        assertEquals("future_timestamp",
                decoder.decode(raw(position("69.5", "17.9", "0", "2026-10-13T10:00:00+00:00"))).reason);
        assertEquals("bad_timestamp", decoder.decode(raw(position("69.5", "17.9", "0", "yesterday"))).reason);
        AisDecoder.Result aton = decoder.decode(raw(
                "{\"type\":\"Aton\",\"mmsi\":992581503,\"msgtime\":\"2026-10-13T09:36:56+00:00\",\"stream\":\"terra\"}"));
        assertFalse(aton.ok());
        assertEquals("unsupported_type", aton.reason);
        assertEquals("terra", aton.stream);
    }
}
