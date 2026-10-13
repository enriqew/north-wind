package io.github.enriqew.northwind.model;

/**
 * Where one input message ended up. Every message gets exactly one, which is what the conservation
 * check sums: processed positions, static reports, duplicates, late and invalid messages add up to
 * the input, or the run fails.
 */
public class Disposition {
    public static final String INPUT = "input";
    public static final String POSITION = "position";
    public static final String STATIC = "static";
    public static final String DUPLICATE = "duplicate";
    public static final String LATE = "late";
    public static final String INVALID = "invalid";
    /**
     * Sent before the window opened or after it closed. A window is cut on arrival time, and late
     * receptions inside it carry event times from before it: satellite messages up to two hours
     * earlier. Kept, those sparse early positions open the window with gaps that never happened.
     */
    public static final String OUTSIDE_WINDOW = "outside_window";

    public String category;
    /** Why, for invalid messages; empty otherwise. */
    public String reason;
    /** Receiver network, kept so lateness and duplication can be broken down by it. */
    public String stream;
    public long msgtimeMs;
    public long arrivalMs;

    public Disposition() {}

    public Disposition(String category, String reason, String stream, long msgtimeMs, long arrivalMs) {
        this.category = category;
        this.reason = reason == null ? "" : reason;
        this.stream = stream == null ? "" : stream;
        this.msgtimeMs = msgtimeMs;
        this.arrivalMs = arrivalMs;
    }

    public static Disposition of(String category, String reason, AisRecord r) {
        return new Disposition(category, reason, r.stream, r.msgtimeMs, r.arrivalMs);
    }

    public String countKey() {
        return category + '|' + reason + '|' + stream;
    }
}
