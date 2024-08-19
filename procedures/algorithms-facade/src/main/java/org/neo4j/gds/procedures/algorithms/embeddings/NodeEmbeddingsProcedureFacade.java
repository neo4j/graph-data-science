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
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsTrainModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.embeddings.fastrp.FastRPStatsConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.FastRPMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.GraphSageMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.HashGnnMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.Node2VecMutateStub;
import org.neo4j.gds.procedures.algorithms.runners.AlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public final class NodeEmbeddingsProcedureFacade {
    private final RequestScopedDependencies requestScopedDependencies;
    private final FastRPMutateStub fastRPMutateStub;
    private final GraphSageMutateStub graphSageMutateStub;
    private final HashGnnMutateStub hashGnnMutateStub;
    private final Node2VecMutateStub node2VecMutateStub;

    private final ApplicationsFacade applicationsFacade;

    private final EstimationModeRunner estimationMode;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;

    private NodeEmbeddingsProcedureFacade(
        RequestScopedDependencies requestScopedDependencies,
        FastRPMutateStub fastRPMutateStub,
        GraphSageMutateStub graphSageMutateStub,
        HashGnnMutateStub hashGnnMutateStub,
        Node2VecMutateStub node2VecMutateStub,
        ApplicationsFacade applicationsFacade,
        EstimationModeRunner estimationMode,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding
    ) {
        this.requestScopedDependencies = requestScopedDependencies;
        this.fastRPMutateStub = fastRPMutateStub;
        this.graphSageMutateStub = graphSageMutateStub;
        this.hashGnnMutateStub = hashGnnMutateStub;
        this.node2VecMutateStub = node2VecMutateStub;
        this.applicationsFacade = applicationsFacade;
        this.estimationMode = estimationMode;
        this.algorithmExecutionScaffolding = algorithmExecutionScaffolding;
    }

    public static NodeEmbeddingsProcedureFacade create(
        RequestScopedDependencies requestScopedDependencies,
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        EstimationModeRunner estimationModeRunner,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding
    ) {
        var fastRPMutateStub = new FastRPMutateStub(genericStub, applicationsFacade);
        var graphSageMutateStub = new GraphSageMutateStub(requestScopedDependencies, genericStub, applicationsFacade);
        var hashGnnMutateStub = new HashGnnMutateStub(genericStub, applicationsFacade);
        var node2VecMutateStub = new Node2VecMutateStub(genericStub, applicationsFacade);

        return new NodeEmbeddingsProcedureFacade(
            requestScopedDependencies,
            fastRPMutateStub,
            graphSageMutateStub,
            hashGnnMutateStub,
            node2VecMutateStub,
            applicationsFacade,
            estimationModeRunner,
            algorithmExecutionScaffolding
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
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

        return algorithmExecutionScaffolding.runStreamAlgorithm(
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

    public GraphSageMutateStub graphSageMutateStub() {
        return graphSageMutateStub;
    }

    public Stream<GraphSageStreamResult> graphSageStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new GraphSageResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            cypherMapWrapper -> GraphSageStreamConfig.of(
                requestScopedDependencies.getUser().getUsername(),
                cypherMapWrapper
            ),
            streamMode()::graphSage,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> graphSageStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            cypherMapWrapper -> GraphSageStreamConfig.of(
                requestScopedDependencies.getUser().getUsername(),
                cypherMapWrapper
            ),
            configuration -> estimationMode().graphSage(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<GraphSageTrainResult> graphSageTrain(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new GraphSageResultBuilderForTrainMode();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            cypherMapWrapper -> GraphSageTrainConfig.of(
                requestScopedDependencies.getUser().getUsername(),
                cypherMapWrapper
            ),
            trainMode()::graphSage,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> graphSageTrainEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            cypherMapWrapper -> GraphSageTrainConfig.of(
                requestScopedDependencies.getUser().getUsername(),
                cypherMapWrapper
            ),
            configuration -> estimationMode().graphSageTrain(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<DefaultNodeEmbeddingsWriteResult> graphSageWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new GraphSageResultBuilderForWriteMode();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            cypherMapWrapper -> GraphSageWriteConfig.of(
                requestScopedDependencies.getUser().getUsername(),
                cypherMapWrapper
            ),
            writeMode()::graphSage,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> graphSageWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            cypherMapWrapper -> GraphSageWriteConfig.of(
                requestScopedDependencies.getUser().getUsername(),
                cypherMapWrapper
            ),
            configuration -> estimationMode().graphSage(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public HashGnnMutateStub hashGnnMutateStub() {
        return hashGnnMutateStub;
    }

    public Stream<HashGNNStreamResult> hashGnnStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new HashGnnResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            HashGNNStreamConfig::of,
            streamMode()::hashGnn,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> hashGnnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            HashGNNStreamConfig::of,
            configuration -> estimationMode().hashGnn(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Node2VecMutateStub node2VecMutateStub() {
        return node2VecMutateStub;
    }

    public Stream<Node2VecStreamResult> node2VecStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new Node2VecResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            Node2VecStreamConfig::of,
            streamMode()::node2Vec,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> node2VecStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            Node2VecStreamConfig::of,
            configuration -> estimationMode().node2Vec(configuration, graphNameOrConfiguration)
        );

        return Stream.of(result);
    }

    public Stream<Node2VecWriteResult> node2VecWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new Node2VecResultBuilderForWriteMode();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphName,
            configuration,
            Node2VecWriteConfig::of,
            writeMode()::node2Vec,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> node2VecWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            Node2VecWriteConfig::of,
            configuration -> estimationMode().node2Vec(configuration, graphNameOrConfiguration)
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

    private NodeEmbeddingAlgorithmsTrainModeBusinessFacade trainMode() {
        return applicationsFacade.nodeEmbeddings().train();
    }

    private NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeMode() {
        return applicationsFacade.nodeEmbeddings().write();
    }
}
