package org.neo4j.gds.ml.pipeline.nodePipeline.train;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.ModelStats;
import org.neo4j.gds.ml.metrics.StatsMap;
import org.neo4j.gds.ml.models.TrainerConfig;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@ValueClass
public interface ModelSelectResult {
    TrainerConfig bestParameters();

    Map<Metric, List<ModelStats>> trainStats();

    Map<Metric, List<ModelStats>> validationStats();

    static ModelSelectResult of(
        TrainerConfig bestConfig,
        StatsMap trainStats,
        StatsMap validationStats
    ) {
        return ImmutableModelSelectResult.of(bestConfig, trainStats.getMap(), validationStats.getMap());
    }

    @Value.Derived
    default Map<String, Object> toMap() {
        Function<Map<Metric, List<ModelStats>>, Map<String, Object>> statsConverter = stats ->
            stats.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().name(),
                value -> value.getValue().stream().map(ModelStats::toMap)
            ));

        return Map.of(
            "bestParameters", bestParameters().toMap(),
            "trainStats", statsConverter.apply(trainStats()),
            "validationStats", statsConverter.apply(validationStats())
        );
    }

}
