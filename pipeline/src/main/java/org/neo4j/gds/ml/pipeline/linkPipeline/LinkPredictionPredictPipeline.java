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
package org.neo4j.gds.ml.pipeline.linkPipeline;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStep;
import org.neo4j.gds.ml.pipeline.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LinkPredictionPredictPipeline implements Pipeline<LinkFeatureStep> {
    public static final LinkPredictionPredictPipeline EMPTY = new LinkPredictionPredictPipeline(List.of(), List.of());

    private final List<ExecutableNodePropertyStep> nodePropertySteps;
    private final List<LinkFeatureStep> featureSteps;

    private LinkPredictionPredictPipeline(
        List<ExecutableNodePropertyStep> nodePropertySteps,
        List<LinkFeatureStep> featureSteps
    ) {
        this.nodePropertySteps = nodePropertySteps;
        this.featureSteps = featureSteps;
    }

    public static LinkPredictionPredictPipeline from(Pipeline<LinkFeatureStep> pipeline) {
        return new LinkPredictionPredictPipeline(
            List.copyOf(pipeline.nodePropertySteps()),
            List.copyOf(pipeline.featureSteps())
        );
    }

    public static LinkPredictionPredictPipeline from(
        Stream<ExecutableNodePropertyStep> nodePropertySteps,
        Stream<LinkFeatureStep> linkFeatureSteps
    ) {
        return new LinkPredictionPredictPipeline(
            nodePropertySteps.collect(Collectors.toList()),
            linkFeatureSteps.collect(Collectors.toList())
        );
    }

    @Override
    public Map<String, Object> toMap() {
        return Map.of(
            "nodePropertySteps", ToMapConvertible.toMap(nodePropertySteps()),
            "featureSteps", ToMapConvertible.toMap(featureSteps())
        );
    }

    @Override
    public List<ExecutableNodePropertyStep> nodePropertySteps() {
        return nodePropertySteps;
    }

    @Override
    public List<LinkFeatureStep> featureSteps() {
        return featureSteps;
    }

    @Override
    public void specificValidateBeforeExecution(GraphStore graphStore) {}
}
