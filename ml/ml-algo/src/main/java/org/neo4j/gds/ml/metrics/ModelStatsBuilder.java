package org.neo4j.gds.ml.metrics;

import org.neo4j.gds.ml.models.TrainerConfig;

import java.util.HashMap;
import java.util.Map;

public class ModelStatsBuilder {
    private final Map<Metric, Double> min;
    private final Map<Metric, Double> max;
    private final Map<Metric, Double> sum;
    private final TrainerConfig modelParams;
    private final int numberOfSplits;

    public ModelStatsBuilder(TrainerConfig modelParams, int numberOfSplits) {
        this.modelParams = modelParams;
        this.numberOfSplits = numberOfSplits;
        this.min = new HashMap<>();
        this.max = new HashMap<>();
        this.sum = new HashMap<>();
    }

    public void update(Metric metric, double value) {
        min.merge(metric, value, Math::min);
        max.merge(metric, value, Math::max);
        sum.merge(metric, value, Double::sum);
    }

    public ModelStats build(Metric metric) {
        return ImmutableModelStats.of(
            modelParams,
            sum.get(metric) / numberOfSplits,
            min.get(metric),
            max.get(metric)
        );
    }
}
