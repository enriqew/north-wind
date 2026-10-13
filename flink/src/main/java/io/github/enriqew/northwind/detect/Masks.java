package io.github.enriqew.northwind.detect;

import io.github.enriqew.northwind.geo.Geo;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Static context the detectors need, built from the recording by {@code northwind.masks} and read
 * once when an operator opens.
 *
 * <ul>
 *   <li>{@code ports.csv}: fine cells where vessels moor or anchor, with the id of the port area
 *       they belong to. Inside these, stopping is a port call, not loitering.
 *   <li>{@code coverage.csv}: coarse cells with reception in nearly every hour of the recording.
 *       Silence inside them is unusual; silence outside them is just the edge of coverage.
 *   <li>{@code outages.csv}: wall-clock intervals when the ingest itself was not connected.
 * </ul>
 */
public final class Masks implements Serializable {
    public static final double PORT_DLAT = 0.01;
    public static final double PORT_DLON = 0.02;
    public static final double COVERAGE_DLAT = 0.1;
    public static final double COVERAGE_DLON = 0.2;

    private final Map<Long, Integer> portCells;
    private final Set<Long> coverageCells;
    private final long[][] outages;

    public Masks(Map<Long, Integer> portCells, Set<Long> coverageCells, long[][] outages) {
        this.portCells = portCells;
        this.coverageCells = coverageCells;
        this.outages = outages;
    }

    public static Masks empty() {
        return new Masks(new HashMap<>(), new HashSet<>(), new long[0][]);
    }

    public static Masks load(Path dir) throws IOException {
        Map<Long, Integer> ports = new HashMap<>();
        for (String[] f : rows(dir.resolve("ports.csv"))) {
            ports.put(Geo.key(Integer.parseInt(f[0]), Integer.parseInt(f[1])), Integer.parseInt(f[2]));
        }
        Set<Long> coverage = new HashSet<>();
        for (String[] f : rows(dir.resolve("coverage.csv"))) {
            coverage.add(Geo.key(Integer.parseInt(f[0]), Integer.parseInt(f[1])));
        }
        List<long[]> out = new ArrayList<>();
        for (String[] f : rows(dir.resolve("outages.csv"))) {
            out.add(new long[] {Long.parseLong(f[0]), Long.parseLong(f[1])});
        }
        return new Masks(ports, coverage, out.toArray(new long[0][]));
    }

    private static List<String[]> rows(Path file) throws IOException {
        List<String[]> rows = new ArrayList<>();
        List<String> lines = Files.readAllLines(file);
        for (int i = 1; i < lines.size(); i++) { // first line is the header
            String line = lines.get(i).trim();
            if (!line.isEmpty()) {
                rows.add(line.split(","));
            }
        }
        return rows;
    }

    /** Port area id at this position, or null when it is not in a port area. */
    public Integer portAt(double lat, double lon) {
        return portCells.get(Geo.cell(lat, lon, PORT_DLAT, PORT_DLON));
    }

    public boolean covered(double lat, double lon) {
        return coverageCells.contains(Geo.cell(lat, lon, COVERAGE_DLAT, COVERAGE_DLON));
    }

    /** True when any ingest outage overlaps [fromMs, toMs]. */
    public boolean outageOverlaps(long fromMs, long toMs) {
        for (long[] o : outages) {
            if (o[0] <= toMs && o[1] >= fromMs) {
                return true;
            }
        }
        return false;
    }

    public int portCellCount() {
        return portCells.size();
    }

    public int coverageCellCount() {
        return coverageCells.size();
    }
}
