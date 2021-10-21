/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NodeClassificationTrainingPipeline implements ToMapConvertible {
    private final List<NodePropertyStep> nodePropertySteps;
    private final List<String> featureProperties;
    private NodeClassificationSplitConfig splitConfig;
    private List<Map<String, Object>> parameterSpace;

    NodeClassificationTrainingPipeline() {
        this.nodePropertySteps = new ArrayList<>();
        this.featureProperties = new ArrayList<>();
        this.splitConfig = NodeClassificationSplitConfig.DEFAULT_CONFIG;
        this.parameterSpace = List.of(NodeLogisticRegressionTrainConfig
            .defaultConfig(featureProperties, "fakeTargetProperty")
            .toMap()
        );
    }

    List<NodePropertyStep> nodePropertySteps() {
        return nodePropertySteps;
    }

    List<String> featureProperties() {
        return featureProperties;
    }

    NodeClassificationSplitConfig splitConfig() {
        return splitConfig;
    }

    List<Map<String, Object>> parameterSpace() {
        return parameterSpace;
    }

    public void setSplitConfig(NodeClassificationSplitConfig splitConfig) {
        this.splitConfig = splitConfig;
    }

    public void setParameterSpace(List<Map<String, Object>> parameterList) {
        this.parameterSpace = parameterList;
    }

    @Override
    public Map<String, Object> toMap() {
        return Map.of(
            "featurePipeline", Map.of(
                "nodePropertySteps", ToMapConvertible.toMap(nodePropertySteps),
                "featureSteps", featureProperties
            ),
            "splitConfig", splitConfig.toMap(),
            "parameterSpace", parameterSpace
        );
    }
}
