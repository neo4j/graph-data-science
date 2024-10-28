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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.pipeline.PipelineCompanion;

import java.util.Map;
import java.util.stream.Stream;

final class LocalNodeRegressionFacade implements NodeRegressionFacade {
    private final NodeRegressionPredictConfigPreProcessor nodeRegressionPredictConfigPreProcessor;

    private final PipelineApplications pipelineApplications;

    private LocalNodeRegressionFacade(
        NodeRegressionPredictConfigPreProcessor nodeRegressionPredictConfigPreProcessor,
        PipelineApplications pipelineApplications
    ) {
        this.nodeRegressionPredictConfigPreProcessor = nodeRegressionPredictConfigPreProcessor;
        this.pipelineApplications = pipelineApplications;
    }

    static NodeRegressionFacade create(
        ModelCatalog modelCatalog,
        User user,
        PipelineApplications pipelineApplications
    ) {
        var nodeRegressionPredictConfigPreProcessor = new NodeRegressionPredictConfigPreProcessor(modelCatalog, user);

        return new LocalNodeRegressionFacade(nodeRegressionPredictConfigPreProcessor, pipelineApplications);
    }

    @Override
    public Stream<PredictMutateResult> mutate(String graphNameAsString, Map<String, Object> configuration) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeRegressionPredictConfigPreProcessor.enhanceInputWithPipelineParameters(configuration);

        var graphName = GraphName.parse(graphNameAsString);

        var result = pipelineApplications.nodeRegressionPredictMutate(
            graphName,
            configuration
        );

        return Stream.of(result);
    }
}
