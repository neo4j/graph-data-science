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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.Computation;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.metrics.regression.RegressionMetrics;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainResult;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PipelineApplicationsTest {
    @Test
    void shouldUseFactoryToConstructRegressionComputation() {
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .databaseId(DatabaseId.DEFAULT)
            .user(User.DEFAULT)
            .build();
        var pipelineConfigurationParser = new PipelineConfigurationParser(requestScopedDependencies.user());
        var algorithmProcessingTemplate = mock(AlgorithmProcessingTemplate.class);
        var nodeRegressionTrainComputationFactory = mock(NodeRegressionTrainComputationFactory.class);
        var pipelineApplications = new PipelineApplications(
            null,
            null,
            null,
            null,
            ModelCatalog.EMPTY,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            requestScopedDependencies.user(),
            null,
            pipelineConfigurationParser,
            null,
            null,
            null,
            null,
            null,
            null,
            algorithmProcessingTemplate,
            null,
            null,
            nodeRegressionTrainComputationFactory
        );

        var configuration = Map.of(
            "graphName",
            "some graph name",
            "metrics",
            List.of(RegressionMetrics.values()[0].name()),
            "modelName",
            "some model name",
            "pipeline",
            "some pipeline",
            "targetProperty",
            "some target property"
        );
        @SuppressWarnings("unchecked") Computation<NodeRegressionTrainResult.NodeRegressionTrainPipelineResult> computation = mock(Computation.class);
        when(nodeRegressionTrainComputationFactory.create(any())).thenReturn(computation);
        pipelineApplications.nodeRegressionTrain(null, configuration);

        verify(algorithmProcessingTemplate).processAlgorithmAndAnySideEffects(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            eq(computation),
            any(),
            any()
        );
    }
}
