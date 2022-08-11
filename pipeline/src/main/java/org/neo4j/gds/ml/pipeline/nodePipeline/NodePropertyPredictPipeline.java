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
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStep;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.ml.pipeline.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class NodePropertyPredictPipeline implements Pipeline<NodeFeatureStep> {
    public static final NodePropertyPredictPipeline EMPTY = new NodePropertyPredictPipeline(List.of(), List.of());

    private final List<ExecutableNodePropertyStep> nodePropertySteps;
    private final List<NodeFeatureStep> featureSteps;


    private NodePropertyPredictPipeline(
        List<ExecutableNodePropertyStep> nodePropertySteps,
        List<NodeFeatureStep> featureSteps
    ) {
        this.nodePropertySteps = nodePropertySteps;
        this.featureSteps = featureSteps;
    }

    public static NodePropertyPredictPipeline from(Pipeline<NodeFeatureStep> pipeline) {
        return new NodePropertyPredictPipeline(
            List.copyOf(pipeline.nodePropertySteps()),
            List.copyOf(pipeline.featureSteps())
        );
    }

    public static NodePropertyPredictPipeline from(
        Stream<NodePropertyStep> nodePropertySteps,
        Stream<NodeFeatureStep> featureSteps
    ) {
        return new NodePropertyPredictPipeline(
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
    public List<NodeFeatureStep> featureSteps() {
        return featureSteps;
    }

    @Override
    public void specificValidateBeforeExecution(GraphStore graphStore) {}

    public List<String> featureProperties() {
        return featureSteps()
            .stream()
            .flatMap(step -> step.inputNodeProperties().stream())
            .collect(Collectors.toList());
    }
}
