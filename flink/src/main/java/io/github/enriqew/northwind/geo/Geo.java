package io.github.enriqew.northwind.geo;

/** Distances and grid cells. The grid formula is mirrored in the Python mask builder and must match it. */
public final class Geo {
    private static final double EARTH_RADIUS_M = 6_371_008.8;
    public static final double MS_TO_KNOTS = 1.943844;

    private Geo() {}

    public static double haversineM(double lat1, double lon1, double lat2, double lon2) {
        double p1 = Math.toRadians(lat1);
        double p2 = Math.toRadians(lat2);
        double dp = p2 - p1;
        double dl = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dp / 2) * Math.sin(dp / 2)
                + Math.cos(p1) * Math.cos(p2) * Math.sin(dl / 2) * Math.sin(dl / 2);
        return 2 * EARTH_RADIUS_M * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }

    /** Speed in knots needed to cover the distance in the time; infinite when no time passed. */
    public static double impliedKnots(double meters, long millis) {
        if (millis <= 0) {
            return meters > 0 ? Double.POSITIVE_INFINITY : 0.0;
        }
        return meters / (millis / 1000.0) * MS_TO_KNOTS;
    }

    public static int row(double lat, double dLat) {
        return (int) Math.floor(lat / dLat);
    }

    public static int col(double lon, double dLon) {
        return (int) Math.floor(lon / dLon);
    }

    public static long key(int row, int col) {
        return ((long) row << 32) ^ (col & 0xffffffffL);
    }

    public static long cell(double lat, double lon, double dLat, double dLon) {
        return key(row(lat, dLat), col(lon, dLon));
    }

    /** Width in metres of one cell of {@code dLon} degrees at this latitude. */
    public static double cellWidthM(double lat, double dLon) {
        return Math.toRadians(dLon) * EARTH_RADIUS_M * Math.cos(Math.toRadians(lat));
    }
}
