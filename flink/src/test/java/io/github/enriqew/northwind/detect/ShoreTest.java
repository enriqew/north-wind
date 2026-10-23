package io.github.enriqew.northwind.detect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ShoreTest {

    private static final double LAT = 69.70;
    private static final double LON = 19.00;
    /** Degrees of latitude per metre. */
    private static final double DEG_PER_M = 1 / 111_195.0;

    /** A quay running east-west along LAT, and a natural coast 300 m north of it. */
    private static Shore shore(Path dir) throws IOException {
        Path f = dir.resolve("shore.csv");
        Files.writeString(f, String.join("\n",
                "kind,lat1,lon1,lat2,lon2",
                "b," + LAT + "," + (LON - 0.01) + "," + LAT + "," + (LON + 0.01),
                "n," + (LAT + 300 * DEG_PER_M) + "," + (LON - 0.05) + "," + (LAT + 300 * DEG_PER_M) + "," + (LON + 0.05),
                ""));
        return Shore.load(f);
    }

    @Test
    void measuresTheDistanceToTheNearestSegment(@TempDir Path dir) throws IOException {
        Shore s = shore(dir);
        assertEquals(30, s.builtM(LAT - 30 * DEG_PER_M, LON), 0.5);
        assertEquals(330, s.naturalM(LAT - 30 * DEG_PER_M, LON), 0.5);
    }

    @Test
    void findsASegmentInTheNextCell(@TempDir Path dir) throws IOException {
        Shore s = shore(dir);
        // The natural coast crosses into the next row of cells; a point just below it still finds it.
        double lat = Math.floor((LAT + 300 * DEG_PER_M) / Masks.PORT_DLAT) * Masks.PORT_DLAT - 20 * DEG_PER_M;
        assertTrue(s.naturalM(lat, LON) < 1_200);
    }

    @Test
    void farFromAnySegmentIsInfinite(@TempDir Path dir) throws IOException {
        assertEquals(Double.POSITIVE_INFINITY, shore(dir).builtM(LAT - 0.1, LON));
    }

    @Test
    void bandsByTheNearestShore() {
        assertEquals(Shore.QUAY, Shore.band(20, 500));
        assertEquals(Shore.COAST, Shore.band(400, 40));
        assertEquals(Shore.NEAR, Shore.band(150, 300));
        assertEquals(Shore.OPEN, Shore.band(Double.POSITIVE_INFINITY, 250));
    }
}
