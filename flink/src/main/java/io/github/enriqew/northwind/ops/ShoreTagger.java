package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.detect.Farms;
import io.github.enriqew.northwind.detect.Shore;
import io.github.enriqew.northwind.model.PatternEvent;
import java.nio.file.Paths;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Stamps loitering and rendezvous with how far they began from the shore, and whether at a fish farm. Stateless and per event,
 * so it does not affect determinism; the coast index is large, so it runs at parallelism 1 and
 * loads it once.
 */
public class ShoreTagger extends RichMapFunction<PatternEvent, PatternEvent> {
    private final String masksDir;
    private transient Shore shore;
    private transient Farms farms;

    public ShoreTagger(String masksDir) {
        this.masksDir = masksDir;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        shore = Shore.load(Paths.get(masksDir, "shore.csv"));
        farms = Farms.load(Paths.get(masksDir, "farms.csv"));
    }

    @Override
    public PatternEvent map(PatternEvent e) {
        if (PatternEvent.LOITERING.equals(e.pattern) || PatternEvent.RENDEZVOUS.equals(e.pattern)) {
            double built = shore.builtM(e.lat, e.lon);
            double nearest = Math.min(built, shore.naturalM(e.lat, e.lon));
            e.shore = Shore.band(built, nearest);
            e.shoreM = Double.isInfinite(nearest) ? null : Math.round(nearest * 10) / 10.0;
            e.atFarm = farms.near(e.lat, e.lon);
        }
        return e;
    }
}
