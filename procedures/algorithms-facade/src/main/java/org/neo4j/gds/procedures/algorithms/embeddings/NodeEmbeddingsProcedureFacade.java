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
package org.neo4j.gds.procedures.algorithms.embeddings;

import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.embeddings.fastrp.FastRPStatsConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteConfig;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.FastRPMutateStub;
import org.neo4j.gds.procedures.algorithms.runners.AlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public final class NodeEmbeddingsProcedureFacade {
    private final FastRPMutateStub fastRPMutateStub;

    private final ApplicationsFacade applicationsFacade;

    private final EstimationModeRunner estimationMode;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffoldingForStreamMode;

    private NodeEmbeddingsProcedureFacade(
        FastRPMutateStub fastRPMutateStub,
        ApplicationsFacade applicationsFacade,
        EstimationModeRunner estimationMode,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding,
        AlgorithmExecutionScaffolding algorithmExecutionScaffoldingForStreamMode
    ) {
        this.fastRPMutateStub = fastRPMutateStub;
        this.applicationsFacade = applicationsFacade;
        this.estimationMode = estimationMode;
        this.algorithmExecutionScaffolding = algorithmExecutionScaffolding;
        this.algorithmExecutionScaffoldingForStreamMode = algorithmExecutionScaffoldingForStreamMode;
    }

    public static NodeEmbeddingsProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        EstimationModeRunner estimationModeRunner,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding,
        AlgorithmExecutionScaffolding algorithmExecutionScaffoldingForStreamMode
    ) {
        var fastRPMutateStub = new FastRPMutateStub(genericStub, applicationsFacade);

        return new NodeEmbeddingsProcedureFacade(
            fastRPMutateStub,
            applicationsFacade,
            estimationModeRunner,
            algorithmExecutionScaffolding,
            algorithmExecutionScaffoldingForStreamMode
        );
    }

    public FastRPMutateStub fastRPMutateStub() {
        return fastRPMutateStub;
    }

    public Stream<FastRPStatsResult> fastRPStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new FastRPResultBuilderForStatsMode();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            FastRPStatsConfig::of,
            statsMode()::fastRP,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> fastRPStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            FastRPStatsConfig::of,
            configuration -> estimationMode().fastRP(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<FastRPStreamResult> fastRPStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new FastRPResultBuilderForStreamMode();

        return algorithmExecutionScaffoldingForStreamMode.runAlgorithm(
            graphName,
            configuration,
            FastRPStreamConfig::of,
            streamMode()::fastRP,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> fastRPStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            FastRPStreamConfig::of,
            configuration -> estimationMode().fastRP(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<DefaultNodeEmbeddingsWriteResult> fastRPWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new FastRPResultBuilderForWriteMode();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            FastRPWriteConfig::of,
            writeMode()::fastRP,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> fastRPWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            FastRPWriteConfig::of,
            configuration -> estimationMode().fastRP(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    private NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationMode() {
        return applicationsFacade.nodeEmbeddings().estimate();
    }

    private NodeEmbeddingAlgorithmsStatsModeBusinessFacade statsMode() {
        return applicationsFacade.nodeEmbeddings().stats();
    }

    private NodeEmbeddingAlgorithmsStreamModeBusinessFacade streamMode() {
        return applicationsFacade.nodeEmbeddings().stream();
    }

    private NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeMode() {
        return applicationsFacade.nodeEmbeddings().write();
    }
}
