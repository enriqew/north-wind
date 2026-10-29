package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * How far a position is from the shore, built ({@code b}: quays, piers, breakwaters) and natural
 * ({@code n}: the coastline), from {@code shore.csv} (see {@code northwind.shore}).
 *
 * <p>Segments are indexed by the port grid; a lookup scans the position's cell and its eight
 * neighbours, so any segment closer than one cell (at least 700 m in Norway) is found. Beyond
 * {@link #NEAR_M} the exact distance does not matter: the position is in open water.
 *
 * <p>A vessel holding position within {@link #SHORE_M} of the shore is moored (or anchored so close
 * in that nothing distinguishes it); beyond {@link #NEAR_M} it is at anchor or holding position at
 * sea. Between the two it cannot be told.
 */
public final class Shore implements Serializable {
    public static final double SHORE_M = 50.0;
    public static final double NEAR_M = 200.0;

    public static final String QUAY = "quay";
    public static final String COAST = "coast";
    public static final String NEAR = "near";
    public static final String OPEN = "open";

    private static final double M_PER_DEG = Math.toRadians(1) * 6_371_008.8;

    /** Per cell, segments packed as lat1, lon1, lat2, lon2; built and natural apart. */
    private final Map<Long, double[]> built;
    private final Map<Long, double[]> natural;

    Shore(Map<Long, double[]> built, Map<Long, double[]> natural) {
        this.built = built;
        this.natural = natural;
    }

    public static Shore empty() {
        return new Shore(new HashMap<>(), new HashMap<>());
    }

    public static Shore load(Path file) throws IOException {
        Map<Long, List<double[]>> b = new HashMap<>();
        Map<Long, List<double[]>> n = new HashMap<>();
        try (BufferedReader in = Files.newBufferedReader(file)) {
            in.readLine(); // header
            String line;
            while ((line = in.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] f = line.split(",");
                add(f[0].equals("b") ? b : n, Double.parseDouble(f[1]), Double.parseDouble(f[2]),
                        Double.parseDouble(f[3]), Double.parseDouble(f[4]));
            }
        }
        return new Shore(pack(b), pack(n));
    }

    /** Files a segment under every cell its bounding box covers. */
    static void add(Map<Long, List<double[]>> index, double lat1, double lon1, double lat2, double lon2) {
        int r0 = Geo.row(Math.min(lat1, lat2), Masks.PORT_DLAT);
        int r1 = Geo.row(Math.max(lat1, lat2), Masks.PORT_DLAT);
        int c0 = Geo.col(Math.min(lon1, lon2), Masks.PORT_DLON);
        int c1 = Geo.col(Math.max(lon1, lon2), Masks.PORT_DLON);
        double[] s = {lat1, lon1, lat2, lon2};
        for (int r = r0; r <= r1; r++) {
            for (int c = c0; c <= c1; c++) {
                index.computeIfAbsent(Geo.key(r, c), k -> new ArrayList<>()).add(s);
            }
        }
    }

    static Map<Long, double[]> pack(Map<Long, List<double[]>> index) {
        Map<Long, double[]> packed = new HashMap<>(index.size() * 2);
        for (Map.Entry<Long, List<double[]>> e : index.entrySet()) {
            double[] a = new double[e.getValue().size() * 4];
            int i = 0;
            for (double[] s : e.getValue()) {
                System.arraycopy(s, 0, a, i, 4);
                i += 4;
            }
            packed.put(e.getKey(), a);
        }
        return packed;
    }

    /** Metres to the nearest built segment, or infinity when none is within a cell. */
    public double builtM(double lat, double lon) {
        return nearest(built, lat, lon);
    }

    /** Metres to the nearest natural segment, or infinity when none is within a cell. */
    public double naturalM(double lat, double lon) {
        return nearest(natural, lat, lon);
    }

    /** Which side of the line a position holding still falls on: quay, coast, near or open. */
    public static String band(double builtM, double naturalM) {
        if (builtM <= SHORE_M) {
            return QUAY;
        }
        double d = Math.min(builtM, naturalM);
        if (d <= SHORE_M) {
            return COAST;
        }
        return d <= NEAR_M ? NEAR : OPEN;
    }

    private static double nearest(Map<Long, double[]> index, double lat, double lon) {
        int row = Geo.row(lat, Masks.PORT_DLAT);
        int col = Geo.col(lon, Masks.PORT_DLON);
        // Local flat projection around the position: within a kilometre it is off by far less than a metre.
        double kx = M_PER_DEG * StrictMath.cos(Math.toRadians(lat));
        double best = Double.POSITIVE_INFINITY;
        for (int r = row - 1; r <= row + 1; r++) {
            for (int c = col - 1; c <= col + 1; c++) {
                double[] a = index.get(Geo.key(r, c));
                if (a == null) {
                    continue;
                }
                for (int i = 0; i < a.length; i += 4) {
                    best = Math.min(best, segmentM(
                            (a[i + 1] - lon) * kx, (a[i] - lat) * M_PER_DEG,
                            (a[i + 3] - lon) * kx, (a[i + 2] - lat) * M_PER_DEG));
                }
            }
        }
        return best;
    }

    /** Distance from the origin to the segment (x1, y1)-(x2, y2). */
    static double segmentM(double x1, double y1, double x2, double y2) {
        double dx = x2 - x1;
        double dy = y2 - y1;
        double len2 = dx * dx + dy * dy;
        double t = len2 == 0 ? 0 : Math.max(0, Math.min(1, -(x1 * dx + y1 * dy) / len2));
        return StrictMath.hypot(x1 + t * dx, y1 + t * dy);
    }
}
