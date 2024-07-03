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
package org.neo4j.gds.procedures.algorithms.embeddings.stubs;

import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.embeddings.DefaultNodeEmbeddingMutateResult;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.procedures.algorithms.stubs.MutateStub;

import java.util.Map;
import java.util.stream.Stream;

public class GraphSageMutateStub implements MutateStub<GraphSageMutateConfig, DefaultNodeEmbeddingMutateResult> {
    private final RequestScopedDependencies requestScopedDependencies;
    private final GenericStub genericStub;
    private final ApplicationsFacade applicationsFacade;

    public GraphSageMutateStub(
        RequestScopedDependencies requestScopedDependencies,
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade
    ) {
        this.requestScopedDependencies = requestScopedDependencies;
        this.genericStub = genericStub;
        this.applicationsFacade = applicationsFacade;
    }

    @Override
    public GraphSageMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(cypherMapWrapper -> GraphSageMutateConfig.of(
            Username.EMPTY_USERNAME.username(),
            cypherMapWrapper
        ), configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> rawConfiguration) {
        return genericStub.getMemoryEstimation(
            username,
            rawConfiguration,
            cypherMapWrapper -> GraphSageMutateConfig.of(username, cypherMapWrapper),
            configuration -> estimationMode().graphSage(configuration, true)
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphNameAsString, Map<String, Object> rawConfiguration) {
        return genericStub.estimate(
            graphNameAsString,
            rawConfiguration,
            cypherMapWrapper -> GraphSageMutateConfig.of(
                requestScopedDependencies.getUser().getUsername(),
                cypherMapWrapper
            ),
            configuration -> estimationMode().graphSage(configuration, true)
        );
    }

    @Override
    public Stream<DefaultNodeEmbeddingMutateResult> execute(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new GraphSageResultBuilderForMutateMode();

        var username = requestScopedDependencies.getUser().getUsername();

        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            cypherMapWrapper -> GraphSageMutateConfig.of(username, cypherMapWrapper),
            applicationsFacade.nodeEmbeddings().mutate()::graphSage,
            resultBuilder
        );
    }

    private NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationMode() {
        return applicationsFacade.nodeEmbeddings().estimate();
    }
}
