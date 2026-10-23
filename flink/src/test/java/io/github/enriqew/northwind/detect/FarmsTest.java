package io.github.enriqew.northwind.detect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FarmsTest {

    private static final double LAT = 69.70;
    private static final double LON = 19.00;
    /** Degrees of latitude per metre. */
    private static final double DEG_PER_M = 1 / 111_195.0;

    /** Two square sites, 400 m on a side: one at LAT, LON, the other well to the east. */
    private static Farms farms(Path dir) throws IOException {
        double s = 400 * DEG_PER_M;
        double w = s / Math.cos(Math.toRadians(LAT));
        StringBuilder csv = new StringBuilder("farm,lat,lon\n");
        for (int farm = 0; farm < 2; farm++) {
            double lon0 = LON + farm * 0.1;
            double[][] corners = {{LAT, lon0}, {LAT, lon0 + w}, {LAT + s, lon0 + w}, {LAT + s, lon0}, {LAT, lon0}};
            for (double[] c : corners) {
                csv.append(farm).append(',').append(c[0]).append(',').append(c[1]).append('\n');
            }
        }
        Path f = dir.resolve("farms.csv");
        Files.writeString(f, csv.toString());
        return Farms.load(f);
    }

    @Test
    void insideASiteIsZero(@TempDir Path dir) throws IOException {
        assertEquals(0, farms(dir).distanceM(LAT + 200 * DEG_PER_M, LON + 0.005));
    }

    @Test
    void measuresTheDistanceOutside(@TempDir Path dir) throws IOException {
        Farms f = farms(dir);
        assertEquals(150, f.distanceM(LAT - 150 * DEG_PER_M, LON + 0.005), 0.5);
        assertTrue(f.near(LAT - 150 * DEG_PER_M, LON + 0.005));
        assertFalse(f.near(LAT - 250 * DEG_PER_M, LON + 0.005));
    }

    @Test
    void readsEverySite(@TempDir Path dir) throws IOException {
        assertEquals(0, farms(dir).distanceM(LAT + 200 * DEG_PER_M, LON + 0.105));
    }
}
