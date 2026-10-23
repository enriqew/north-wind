package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Licensed fish farm sites, from {@code farms.csv} (see {@code northwind.shore}). A vessel holding
 * position within {@link #FARM_M} of one, or a pair doing so, is most likely a well boat or a feed
 * carrier at work.
 *
 * <p>Sites are indexed by the port grid, like {@link Shore}: a lookup checks the sites filed under the
 * position's cell and its neighbours, so a site within a cell's width is found.
 */
public final class Farms implements Serializable {
    public static final double FARM_M = 200.0;

    private static final double M_PER_DEG = Math.toRadians(1) * 6_371_008.8;

    /** Outlines as lat, lon pairs, closed (the last vertex repeats the first). */
    private final List<double[]> outlines;
    private final Map<Long, int[]> index;

    Farms(List<double[]> outlines) {
        this.outlines = outlines;
        Map<Long, List<Integer>> cells = new HashMap<>();
        for (int i = 0; i < outlines.size(); i++) {
            double[] o = outlines.get(i);
            double latMin = Double.MAX_VALUE, latMax = -Double.MAX_VALUE, lonMin = Double.MAX_VALUE, lonMax = -Double.MAX_VALUE;
            for (int k = 0; k < o.length; k += 2) {
                latMin = Math.min(latMin, o[k]);
                latMax = Math.max(latMax, o[k]);
                lonMin = Math.min(lonMin, o[k + 1]);
                lonMax = Math.max(lonMax, o[k + 1]);
            }
            for (int r = Geo.row(latMin, Masks.PORT_DLAT); r <= Geo.row(latMax, Masks.PORT_DLAT); r++) {
                for (int c = Geo.col(lonMin, Masks.PORT_DLON); c <= Geo.col(lonMax, Masks.PORT_DLON); c++) {
                    cells.computeIfAbsent(Geo.key(r, c), x -> new ArrayList<>()).add(i);
                }
            }
        }
        this.index = new HashMap<>();
        cells.forEach((k, v) -> index.put(k, v.stream().mapToInt(Integer::intValue).toArray()));
    }

    public static Farms empty() {
        return new Farms(new ArrayList<>());
    }

    public static Farms load(Path file) throws IOException {
        List<double[]> outlines = new ArrayList<>();
        List<Double> current = new ArrayList<>();
        String farm = null;
        List<String> lines = Files.readAllLines(file);
        for (int i = 1; i < lines.size(); i++) { // first line is the header
            String line = lines.get(i).trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] f = line.split(",");
            if (!f[0].equals(farm)) {
                close(outlines, current);
                farm = f[0];
            }
            current.add(Double.parseDouble(f[1]));
            current.add(Double.parseDouble(f[2]));
        }
        close(outlines, current);
        return new Farms(outlines);
    }

    private static void close(List<double[]> outlines, List<Double> current) {
        if (current.size() >= 6) {
            outlines.add(current.stream().mapToDouble(Double::doubleValue).toArray());
        }
        current.clear();
    }

    /** Metres to the nearest site, zero inside one, infinity when none is within a cell. */
    public double distanceM(double lat, double lon) {
        int row = Geo.row(lat, Masks.PORT_DLAT);
        int col = Geo.col(lon, Masks.PORT_DLON);
        double kx = M_PER_DEG * Math.cos(Math.toRadians(lat));
        double best = Double.POSITIVE_INFINITY;
        for (int r = row - 1; r <= row + 1; r++) {
            for (int c = col - 1; c <= col + 1; c++) {
                int[] ids = index.get(Geo.key(r, c));
                if (ids == null) {
                    continue;
                }
                for (int id : ids) {
                    double[] o = outlines.get(id);
                    if (inside(o, lat, lon)) {
                        return 0;
                    }
                    for (int k = 0; k + 3 < o.length; k += 2) {
                        best = Math.min(best, Shore.segmentM(
                                (o[k + 1] - lon) * kx, (o[k] - lat) * M_PER_DEG,
                                (o[k + 3] - lon) * kx, (o[k + 2] - lat) * M_PER_DEG));
                    }
                }
            }
        }
        return best;
    }

    public boolean near(double lat, double lon) {
        return distanceM(lat, lon) <= FARM_M;
    }

    /** Even-odd ray cast, eastwards. */
    static boolean inside(double[] o, double lat, double lon) {
        boolean in = false;
        for (int k = 0, j = o.length - 2; k < o.length; j = k, k += 2) {
            if ((o[k] > lat) != (o[j] > lat)
                    && lon < (o[j + 1] - o[k + 1]) * (lat - o[k]) / (o[j] - o[k]) + o[k + 1]) {
                in = !in;
            }
        }
        return in;
    }
}
