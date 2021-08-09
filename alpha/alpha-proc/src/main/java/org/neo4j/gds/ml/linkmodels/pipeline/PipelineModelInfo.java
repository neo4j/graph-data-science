/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model.Mappable;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PipelineModelInfo implements Mappable {
    private final List<NodePropertyStep> nodePropertySteps;
    private final List<LinkFeatureStep> featureSteps;
    private LinkPredictionSplitConfig splitConfig;
    // List of specific parameter combinations (in the future also a map with value ranges for different parameters will be allowed)
    // currently only storing the parameters as Map to avoid default concurrency issue
    // TODO resolve default/user-defined concurrency issue and store actual config objects
    private @NotNull List<Map<String, Object>> parameterSpace;

    public PipelineModelInfo() {
        this.nodePropertySteps = new ArrayList<>();
        this.featureSteps = new ArrayList<>();
        this.splitConfig = LinkPredictionSplitConfig.of(CypherMapWrapper.empty());
        this.parameterSpace = List.of(LinkLogisticRegressionTrainConfig
            .of(ConcurrencyConfig.DEFAULT_CONCURRENCY, Map.of())
            .toMap());
    }

    @Override
    public Map<String, Object> toMap() {
        return Map.of(
            "featurePipeline", Map.of(
                "nodePropertySteps", Mappable.toMap(nodePropertySteps),
                "featureSteps", Mappable.toMap(featureSteps)
            ),
            "splitConfig", splitConfig,
            "parameterSpace", parameterSpace
        );
    }

    List<NodePropertyStep> nodePropertySteps() {
        return nodePropertySteps;
    }

    void addNodePropertyStep(String name, Map<String, Object> config) {
        this.nodePropertySteps.add(new NodePropertyStep(name, config));
    }

    List<LinkFeatureStep> featureSteps() {
        return featureSteps;
    }

    void addFeatureStep(String name, Map<String, Object> config) {
        featureSteps.add(LinkFeatureStepFactory.create(name, config));
    }

    LinkPredictionSplitConfig splitConfig() {
        return splitConfig;
    }

    void setSplitConfig(@NotNull LinkPredictionSplitConfig splitConfig) {
        this.splitConfig = splitConfig;
    }

    List<Map<String, Object>> parameterSpace() {
        return parameterSpace;
    }

    void setParameterSpace(@NotNull List<Map<String, Object>> parameterList) {
        this.parameterSpace = parameterList.stream()
            .map(trainParams -> {
                var validatedConfig = LinkLogisticRegressionTrainConfig.of(
                    ConcurrencyConfig.DEFAULT_CONCURRENCY,
                    trainParams
                ).toMap();

                // The concurrency from `train` should be used
                // if not specified otherwise by the user
                if (!trainParams.containsKey(ConcurrencyConfig.CONCURRENCY_KEY)) {
                    validatedConfig.remove(ConcurrencyConfig.CONCURRENCY_KEY);
                }

                return validatedConfig;
            }).collect(Collectors.toList());
    }
}
