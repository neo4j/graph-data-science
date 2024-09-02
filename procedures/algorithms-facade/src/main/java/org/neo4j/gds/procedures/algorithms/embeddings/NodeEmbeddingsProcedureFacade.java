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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsTrainModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.fastrp.FastRPStatsConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.FastRPMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.GraphSageMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.HashGnnMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.Node2VecMutateStub;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public final class NodeEmbeddingsProcedureFacade {

    private final FastRPMutateStub fastRPMutateStub;
    private final GraphSageMutateStub graphSageMutateStub;
    private final HashGnnMutateStub hashGnnMutateStub;
    private final Node2VecMutateStub node2VecMutateStub;

    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final NodeEmbeddingAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade;
    private final NodeEmbeddingAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade;
    private final NodeEmbeddingAlgorithmsTrainModeBusinessFacade trainModeBusinessFacade;
    private final NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade;

    private final UserSpecificConfigurationParser configurationParser;
    private final User user;

    private NodeEmbeddingsProcedureFacade(
        FastRPMutateStub fastRPMutateStub,
        GraphSageMutateStub graphSageMutateStub,
        HashGnnMutateStub hashGnnMutateStub,
        Node2VecMutateStub node2VecMutateStub,
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        NodeEmbeddingAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade,
        NodeEmbeddingAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade,
        NodeEmbeddingAlgorithmsTrainModeBusinessFacade trainModeBusinessFacade,
        NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade,
        UserSpecificConfigurationParser configurationParser,
        User user
    ) {
        this.fastRPMutateStub = fastRPMutateStub;
        this.graphSageMutateStub = graphSageMutateStub;
        this.hashGnnMutateStub = hashGnnMutateStub;
        this.node2VecMutateStub = node2VecMutateStub;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.statsModeBusinessFacade = statsModeBusinessFacade;
        this.streamModeBusinessFacade = streamModeBusinessFacade;
        this.trainModeBusinessFacade = trainModeBusinessFacade;
        this.writeModeBusinessFacade = writeModeBusinessFacade;
        this.configurationParser = configurationParser;
        this.user = user;
    }

    public static NodeEmbeddingsProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        UserSpecificConfigurationParser configurationParser,
        User user
    ) {
        var fastRPMutateStub = new FastRPMutateStub(
            genericStub,
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().mutate()
        );

        var graphSageMutateStub = new GraphSageMutateStub(
            user,
            genericStub,
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().mutate()
        );

        var hashGnnMutateStub = new HashGnnMutateStub(
            genericStub,
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().mutate()
        );

        var node2VecMutateStub = new Node2VecMutateStub(
            genericStub,
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().mutate()
        );

        return new NodeEmbeddingsProcedureFacade(
            fastRPMutateStub,
            graphSageMutateStub,
            hashGnnMutateStub,
            node2VecMutateStub,
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().stats(),
            applicationsFacade.nodeEmbeddings().stream(),
            applicationsFacade.nodeEmbeddings().train(),
            applicationsFacade.nodeEmbeddings().write(),
            configurationParser,
            user
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

        return statsModeBusinessFacade.fastRP(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, FastRPStatsConfig::of),
            resultBuilder
        );

    }

    public Stream<MemoryEstimateResult> fastRPStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.fastRP(
            configurationParser.parseConfiguration(algorithmConfiguration, FastRPStatsConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    public Stream<FastRPStreamResult> fastRPStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new FastRPResultBuilderForStreamMode();

        return streamModeBusinessFacade.fastRP(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, FastRPStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> fastRPStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.fastRP(
            configurationParser.parseConfiguration(algorithmConfiguration, FastRPStreamConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    public Stream<DefaultNodeEmbeddingsWriteResult> fastRPWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new FastRPResultBuilderForWriteMode();

        return writeModeBusinessFacade.fastRP(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, FastRPWriteConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> fastRPWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.fastRP(
            configurationParser.parseConfiguration(algorithmConfiguration, FastRPWriteConfig::of),
            graphNameOrConfiguration
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

        Function<CypherMapWrapper, GraphSageStreamConfig> configFunction = (cypherMapWrapper) -> GraphSageStreamConfig.of(
            user.getUsername(),
            cypherMapWrapper
        );
        return streamModeBusinessFacade.graphSage(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, configFunction),
            resultBuilder
        );

    }

    public Stream<MemoryEstimateResult> graphSageStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        Function<CypherMapWrapper, GraphSageStreamConfig> configFunction = (cypherMapWrapper) -> GraphSageStreamConfig.of(
            user.getUsername(),
            cypherMapWrapper
        );
        var result = estimationModeBusinessFacade.graphSage(
            configurationParser.parseConfiguration(algorithmConfiguration, configFunction),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    public Stream<GraphSageTrainResult> graphSageTrain(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new GraphSageResultBuilderForTrainMode();

        Function<CypherMapWrapper, GraphSageTrainConfig> configFunction = (cypherMapWrapper) -> GraphSageTrainConfig.of(
            user.getUsername(),
            cypherMapWrapper
        );
        return trainModeBusinessFacade.graphSage(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, configFunction),
            resultBuilder
        );

    }

    public Stream<MemoryEstimateResult> graphSageTrainEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        Function<CypherMapWrapper, GraphSageTrainConfig> configFunction = (cypherMapWrapper) -> GraphSageTrainConfig.of(
            user.getUsername(),
            cypherMapWrapper
        );
        var result = estimationModeBusinessFacade.graphSageTrain(
            configurationParser.parseConfiguration(algorithmConfiguration, configFunction),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    public Stream<DefaultNodeEmbeddingsWriteResult> graphSageWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new GraphSageResultBuilderForWriteMode();

        Function<CypherMapWrapper, GraphSageWriteConfig> configFunction = (cypherMapWrapper) -> GraphSageWriteConfig.of(
            user.getUsername(),
            cypherMapWrapper
        );
        return writeModeBusinessFacade.graphSage(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, configFunction),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> graphSageWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        Function<CypherMapWrapper, GraphSageWriteConfig> configFunction = (cypherMapWrapper) -> GraphSageWriteConfig.of(
            user.getUsername(),
            cypherMapWrapper
        );
        var result = estimationModeBusinessFacade.graphSage(
            configurationParser.parseConfiguration(algorithmConfiguration, configFunction),
            graphNameOrConfiguration
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

        return streamModeBusinessFacade.hashGnn(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, HashGNNStreamConfig::of),
            resultBuilder
        );

    }

    public Stream<MemoryEstimateResult> hashGnnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.hashGnn(
            configurationParser.parseConfiguration(algorithmConfiguration, HashGNNStreamConfig::of),
            graphNameOrConfiguration
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

        return streamModeBusinessFacade.node2Vec(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, Node2VecStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> node2VecStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.node2Vec(
            configurationParser.parseConfiguration(algorithmConfiguration, Node2VecStreamConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    public Stream<Node2VecWriteResult> node2VecWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new Node2VecResultBuilderForWriteMode();

        return writeModeBusinessFacade.node2Vec(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, Node2VecWriteConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> node2VecWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.node2Vec(
            configurationParser.parseConfiguration(algorithmConfiguration, Node2VecWriteConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }
}
