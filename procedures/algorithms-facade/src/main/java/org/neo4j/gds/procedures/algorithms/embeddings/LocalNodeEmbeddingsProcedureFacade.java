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
import org.neo4j.gds.embeddings.hashgnn.HashGNNWriteConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.LocalFastRPMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.LocalGraphSageMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.LocalHashGnnMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.LocalNode2VecMutateStub;
import org.neo4j.gds.procedures.algorithms.embeddings.stubs.NodeEmbeddingsStubs;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public final class LocalNodeEmbeddingsProcedureFacade implements NodeEmbeddingsProcedureFacade {

    private final NodeEmbeddingsStubs stubs;
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final NodeEmbeddingAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade;
    private final NodeEmbeddingAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade;
    private final NodeEmbeddingAlgorithmsTrainModeBusinessFacade trainModeBusinessFacade;
    private final NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade;
    private final UserSpecificConfigurationParser configurationParser;
    private final User user;

    private LocalNodeEmbeddingsProcedureFacade(
        NodeEmbeddingsStubs stubs,
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        NodeEmbeddingAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade,
        NodeEmbeddingAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade,
        NodeEmbeddingAlgorithmsTrainModeBusinessFacade trainModeBusinessFacade,
        NodeEmbeddingAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade,
        UserSpecificConfigurationParser configurationParser,
        User user
    ) {
        this.stubs = stubs;
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
        var fastRPMutateStub = new LocalFastRPMutateStub(
            genericStub,
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().mutate()
        );

        var graphSageMutateStub = new LocalGraphSageMutateStub(
            user,
            genericStub,
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().mutate()
        );

        var hashGnnMutateStub = new LocalHashGnnMutateStub(
            genericStub,
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().mutate()
        );

        var node2VecMutateStub = new LocalNode2VecMutateStub(
            genericStub,
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().mutate()
        );

        return new LocalNodeEmbeddingsProcedureFacade(
            new NodeEmbeddingsStubs(
                fastRPMutateStub,
                graphSageMutateStub,
                hashGnnMutateStub,
                node2VecMutateStub
            ),
            applicationsFacade.nodeEmbeddings().estimate(),
            applicationsFacade.nodeEmbeddings().stats(),
            applicationsFacade.nodeEmbeddings().stream(),
            applicationsFacade.nodeEmbeddings().train(),
            applicationsFacade.nodeEmbeddings().write(),
            configurationParser,
            user
        );
    }


    @Override
    public NodeEmbeddingsStubs nodeEmbeddingStubs() {
        return stubs;
    }

    @Override
    public Stream<FastRPStatsResult> fastRPStats(
        String graphName,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = configurationParser.parseConfiguration(rawConfiguration, FastRPStatsConfig::of);
        var resultBuilder = new FastRPResultBuilderForStatsMode(configuration);

        return statsModeBusinessFacade.fastRP(
            GraphName.parse(graphName),
            configuration,
            resultBuilder
        );
    }

    @Override
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

    @Override
    public Stream<DefaultNodeEmbeddingsStreamResult> fastRPStream(
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

    @Override
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

    @Override
    public Stream<DefaultNodeEmbeddingMutateResult> fastRPMutate(String graphName, Map<String, Object> configuration) {
        return stubs.fastRP().execute(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> fastRPMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.fastRP().estimate(graphNameOrConfiguration, algorithmConfiguration);

    }

    @Override
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

    @Override
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


    @Override
    public Stream<DefaultNodeEmbeddingsStreamResult> graphSageStream(
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

    @Override
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

    @Override
    public Stream<DefaultNodeEmbeddingMutateResult> graphSageMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return stubs.graphSage().execute(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> graphSageMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.graphSage().estimate(graphNameOrConfiguration, algorithmConfiguration);
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
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


    @Override
    public Stream<DefaultNodeEmbeddingsStreamResult> hashGnnStream(
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

    @Override
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

    @Override
    public Stream<DefaultNodeEmbeddingMutateResult> hashGnnMutate(String graphName, Map<String, Object> configuration) {
        return stubs.hashGnn().execute(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> hashGnnMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return  stubs.hashGnn().estimate(graphNameOrConfiguration,algorithmConfiguration);
    }

    @Override
    public Stream<DefaultNodeEmbeddingsWriteResult> hashGnnWrite(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new HashGNNResultBuilderForWriteMode();
        return writeModeBusinessFacade.hashGnn(
            GraphName.parse(graphName),
            configurationParser.parseConfiguration(configuration, HashGNNWriteConfig::of),
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> hashGnnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.hashGnn(
            configurationParser.parseConfiguration(algorithmConfiguration, HashGNNWriteConfig::of),
            graphNameOrConfiguration
        );
        return Stream.of(result);
    }

    @Override
    public Stream<DefaultNodeEmbeddingsStreamResult> node2VecStream(
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

    @Override
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

    @Override
    public Stream<Node2VecMutateResult> node2VecMutate(String graphName, Map<String, Object> configuration) {
        return stubs.node2Vec().execute(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> node2VecMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.node2Vec().estimate(graphNameOrConfiguration, algorithmConfiguration);
    }

    @Override
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

    @Override
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
