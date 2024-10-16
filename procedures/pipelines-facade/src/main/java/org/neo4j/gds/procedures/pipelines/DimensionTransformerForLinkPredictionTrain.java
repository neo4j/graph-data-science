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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;

class DimensionTransformerForLinkPredictionTrain implements DimensionTransformer {
    private final PipelineRepository pipelineRepository;
    private final LinkPredictionTrainConfig configuration;

    DimensionTransformerForLinkPredictionTrain(
        PipelineRepository pipelineRepository,
        LinkPredictionTrainConfig configuration
    ) {
        this.pipelineRepository = pipelineRepository;
        this.configuration = configuration;
    }

    @Override
    public GraphDimensions transform(GraphDimensions graphDimensions) {
        // inject expected relationship set sizes which are used in the estimation of the TrainPipelineExecutor
        // this allows to compute the MemoryTree over a single graphDimension
        var user = new User(configuration.username(), false);
        var pipelineName = PipelineName.parse(configuration.pipeline());

        var pipeline = pipelineRepository.getLinkPredictionTrainingPipeline(user, pipelineName);

        var splitConfig = pipeline.splitConfig();
        var targetRelationshipType = configuration.targetRelationshipType();

        return splitConfig.expectedGraphDimensions(graphDimensions, targetRelationshipType);
    }
}
