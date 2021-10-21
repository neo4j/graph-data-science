/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.ml.nodemodels.MetricData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ValueClass
public interface NodeClassificationPipelineModelInfo extends ToMapConvertible {

    NodeClassificationTrainingPipeline trainingPipeline();

    /**
     * The distinct values of the target property which represent the
     * allowed classes that the model can predict.
     * @return
     */
    List<Long> classes();

    /**
     * The parameters that yielded the best fold-averaged validation score
     * for the selection metric.
     * @return
     */
    NodeLogisticRegressionTrainConfig bestParameters();
    Map<Metric, MetricData<NodeLogisticRegressionTrainConfig>> metrics();

    @Override
    @Value.Auxiliary
    @Value.Derived
    default Map<String, Object> toMap() {
        return Map.of(
            "bestParameters", bestParameters().toMap(),
            "classes", classes(),
            "metrics", metrics().entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> entry.getValue().toMap()
            )),
            "trainingPipeline", trainingPipeline().toMap()
        );
    }

    static ImmutableNodeClassificationPipelineModelInfo.Builder builder() {
        return ImmutableNodeClassificationPipelineModelInfo.builder();
    }
}
