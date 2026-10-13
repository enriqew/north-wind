package io.github.enriqew.northwind.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class GeoTest {
    @Test
    void haversineMatchesAKnownDistance() {
        // One degree of latitude is ~111.2 km.
        assertEquals(111_195, Geo.haversineM(70.0, 20.0, 71.0, 20.0), 50);
    }

    @Test
    void gridRowsAndColumnsFloorNegativesToo() {
        assertEquals(6965, Geo.row(69.655, 0.01));
        assertEquals(948, Geo.col(18.965, 0.02));
        assertEquals(-1, Geo.col(-0.001, 0.02));
    }

    /** The Python mask builder writes these exact cells; see py/tests/test_masks.py. */
    @Test
    void cellsMatchThePythonMaskBuilder() {
        assertEquals(Geo.key(6965, 948), Geo.cell(69.655, 18.965, 0.01, 0.02));
        assertEquals(Geo.key(696, 94), Geo.cell(69.655, 18.965, 0.1, 0.2));
    }
}
