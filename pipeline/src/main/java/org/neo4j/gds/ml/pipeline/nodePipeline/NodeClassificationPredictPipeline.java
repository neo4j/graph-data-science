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
import org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStep;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.ml.pipeline.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class NodeClassificationPredictPipeline implements Pipeline<NodeClassificationFeatureStep> {
    public static final NodeClassificationPredictPipeline EMPTY = new NodeClassificationPredictPipeline(List.of(), List.of());

    private final List<ExecutableNodePropertyStep> nodePropertySteps;
    private final List<NodeClassificationFeatureStep> featureSteps;


    private NodeClassificationPredictPipeline(
        List<ExecutableNodePropertyStep> nodePropertySteps,
        List<NodeClassificationFeatureStep> featureSteps
    ) {
        this.nodePropertySteps = nodePropertySteps;
        this.featureSteps = featureSteps;
    }

    public static NodeClassificationPredictPipeline from(Pipeline<NodeClassificationFeatureStep> pipeline) {
        return new NodeClassificationPredictPipeline(
            List.copyOf(pipeline.nodePropertySteps()),
            List.copyOf(pipeline.featureSteps())
        );
    }

    public static NodeClassificationPredictPipeline from(
        Stream<NodePropertyStep> nodePropertySteps,
        Stream<NodeClassificationFeatureStep> featureSteps
    ) {
        return new NodeClassificationPredictPipeline(
            nodePropertySteps.collect(Collectors.toList()),
            featureSteps.collect(Collectors.toList())
        );
    }

    @Override
    public Map<String, Object> toMap() {
        return Map.of(
            "nodePropertySteps", ToMapConvertible.toMap(nodePropertySteps()),
            "featureProperties", ToMapConvertible.toMap(featureSteps())
        );
    }

    @Override
    public List<ExecutableNodePropertyStep> nodePropertySteps() {
        return nodePropertySteps;
    }

    @Override
    public List<NodeClassificationFeatureStep> featureSteps() {
        return featureSteps;
    }

    @Override
    public void specificValidateBeforeExecution(GraphStore graphStore, AlgoBaseConfig config) {}

    public List<String> featureProperties() {
        return featureSteps()
            .stream()
            .flatMap(step -> step.inputNodeProperties().stream())
            .collect(Collectors.toList());
    }
}
