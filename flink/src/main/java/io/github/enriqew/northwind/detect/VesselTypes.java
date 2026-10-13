package io.github.enriqew.northwind.detect;

import java.util.Set;

/**
 * AIS ship-type codes for vessels whose job is to stop at sea or to come alongside others. For
 * them, slow speed away from port and closeness to another vessel are the work itself, so they are
 * left out of loitering and rendezvous rather than flagged.
 */
public final class VesselTypes {
    private VesselTypes() {}

    /** Fishing, towing, dredging, diving, military, pilot, SAR, tug, port tender, anti-pollution, law enforcement, medical. */
    private static final Set<Integer> WORKS_AT_LOW_SPEED =
            Set.of(30, 31, 32, 33, 34, 35, 50, 51, 52, 53, 54, 55, 58);

    /** Towing, pilot, SAR, tug, port tender, anti-pollution, law enforcement: they come alongside by trade. */
    private static final Set<Integer> COMES_ALONGSIDE = Set.of(31, 32, 50, 51, 52, 53, 54, 55);

    public static boolean worksAtLowSpeed(Integer shipType) {
        return shipType != null && WORKS_AT_LOW_SPEED.contains(shipType);
    }

    public static boolean comesAlongside(Integer shipType) {
        return shipType != null && COMES_ALONGSIDE.contains(shipType);
    }
}
