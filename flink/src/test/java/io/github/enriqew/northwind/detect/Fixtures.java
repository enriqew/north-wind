package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import io.github.enriqew.northwind.model.AisRecord;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class Fixtures {
    static final long T0 = 1_791_878_400_000L; // 2026-10-13T00:00:00Z
    static final long MIN = 60_000L;

    private Fixtures() {}

    static AisRecord pos(long mmsi, long t, double lat, double lon, Double sog) {
        AisRecord r = new AisRecord();
        r.kind = AisRecord.POSITION;
        r.messageType = 1;
        r.mmsi = mmsi;
        r.msgtimeMs = t;
        r.arrivalMs = t;
        r.offset = t;
        r.stream = "terra";
        r.lat = lat;
        r.lon = lon;
        r.sog = sog;
        return r;
    }

    /** A port area of one cell at (lat, lon) with id 7, coverage over a 1x1 degree box around it. */
    static Masks masksWithPortAt(double lat, double lon) {
        Map<Long, Integer> ports = new HashMap<>();
        ports.put(Geo.cell(lat, lon, Masks.PORT_DLAT, Masks.PORT_DLON), 7);
        Set<Long> coverage = new HashSet<>();
        for (double a = lat - 1; a <= lat + 1; a += Masks.COVERAGE_DLAT / 2) {
            for (double b = lon - 1; b <= lon + 1; b += Masks.COVERAGE_DLON / 2) {
                coverage.add(Geo.cell(a, b, Masks.COVERAGE_DLAT, Masks.COVERAGE_DLON));
            }
        }
        return new Masks(ports, coverage, new long[0][]);
    }
}
