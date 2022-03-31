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
package org.neo4j.gds.ml.pipeline.nodePipeline;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NodeClassificationTrainingPipeline extends TrainingPipeline<NodeClassificationFeatureStep> {
    public static final String PIPELINE_TYPE = "Node classification training pipeline";
    public static final String MODEL_TYPE = "NodeClassification";


    private NodeClassificationSplitConfig splitConfig;

    public NodeClassificationTrainingPipeline() {
        super();
        this.splitConfig = NodeClassificationSplitConfig.DEFAULT_CONFIG;
    }

    public NodeClassificationTrainingPipeline copy() {
        var copied = new NodeClassificationTrainingPipeline();
        copied.featureSteps.addAll(featureSteps);
        copied.nodePropertySteps.addAll(nodePropertySteps);

        trainingParameterSpace().forEach((key, value) ->
            copied.setTrainingParameterSpace(key, new ArrayList<>(value))
        );

        copied.setSplitConfig(splitConfig);
        return copied;
    }


    @Override
    public String type() {
        return PIPELINE_TYPE;
    }

    @Override
    protected Map<String, List<Map<String, Object>>> featurePipelineDescription() {
        return Map.of(
            "nodePropertySteps", ToMapConvertible.toMap(nodePropertySteps),
            "featureProperties", ToMapConvertible.toMap(featureSteps)
        );
    }

    @Override
    protected Map<String, Object> additionalEntries() {
        return Map.of(
            "splitConfig", splitConfig.toMap()
        );
    }

    public void setSplitConfig(NodeClassificationSplitConfig splitConfig) {
        this.splitConfig = splitConfig;
    }

    public NodeClassificationSplitConfig splitConfig() {
        return splitConfig;
    }

    public List<String> featureProperties() {
        return featureSteps()
            .stream()
            .flatMap(step -> step.inputNodeProperties().stream())
            .collect(Collectors.toList());
    }

    @Override
    public void specificValidateBeforeExecution(GraphStore graphStore, AlgoBaseConfig config) {}
}
