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
package org.neo4j.gds.procedures.algorithms.similarity;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStatsConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStreamConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnWriteConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStatsConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStreamConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityWriteConfig;
import org.neo4j.gds.similarity.knn.KnnStatsConfig;
import org.neo4j.gds.similarity.knn.KnnStreamConfig;
import org.neo4j.gds.similarity.knn.KnnWriteConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStatsConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStreamConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityWriteConfig;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public final class SimilarityProcedureFacade {
    private final ProcedureReturnColumns procedureReturnColumns;

    private final SimilarityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final SimilarityAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade;
    private final SimilarityAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade;
    private final SimilarityAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade;

    //stubs
    private final FilteredKnnMutateStub filteredKnnMutateStub;
    private final FilteredNodeSimilarityMutateStub filteredNodeSimilarityMutateStub;
    private final KnnMutateStub knnMutateStub;
    private final NodeSimilarityMutateStub nodeSimilarityMutateStub;

    private final ConfigurationParser configurationParser;
    private final User user;

    private SimilarityProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        SimilarityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        SimilarityAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade,
        SimilarityAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade,
        SimilarityAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade,
        FilteredKnnMutateStub filteredKnnMutateStub,
        FilteredNodeSimilarityMutateStub filteredNodeSimilarityMutateStub,
        KnnMutateStub knnMutateStub,
        NodeSimilarityMutateStub nodeSimilarityMutateStub,
        ConfigurationParser configurationParser,
        User user
    ) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.statsModeBusinessFacade = statsModeBusinessFacade;
        this.streamModeBusinessFacade = streamModeBusinessFacade;
        this.writeModeBusinessFacade = writeModeBusinessFacade;
        this.filteredKnnMutateStub = filteredKnnMutateStub;
        this.filteredNodeSimilarityMutateStub = filteredNodeSimilarityMutateStub;
        this.knnMutateStub = knnMutateStub;
        this.nodeSimilarityMutateStub = nodeSimilarityMutateStub;
        this.configurationParser = configurationParser;
        this.user = user;
    }

    public static SimilarityProcedureFacade create(
        ApplicationsFacade applicationsFacade,
        GenericStub genericStub,
        ProcedureReturnColumns procedureReturnColumns,
        ConfigurationParser configurationParser,
        User user
    ) {
        var filteredKnnMutateStub = new FilteredKnnMutateStub(
            genericStub,
            applicationsFacade.similarity().estimate(),
            applicationsFacade.similarity().mutate(),
            procedureReturnColumns
        );

        var filteredNodeSimilarityMutateStub = new FilteredNodeSimilarityMutateStub(
            genericStub,
            applicationsFacade.similarity().estimate(),
            applicationsFacade.similarity().mutate(),
            procedureReturnColumns
        );

        var knnMutateStub = new KnnMutateStub(
            genericStub,
            applicationsFacade.similarity().estimate(),
            applicationsFacade.similarity().mutate(),
            procedureReturnColumns
        );

        var nodeSimilarityMutateStub = new NodeSimilarityMutateStub(
            genericStub,
            applicationsFacade.similarity().estimate(),
            applicationsFacade.similarity().mutate(),
            procedureReturnColumns
        );

        return new SimilarityProcedureFacade(
            procedureReturnColumns,
            applicationsFacade.similarity().estimate(),
            applicationsFacade.similarity().stats(),
            applicationsFacade.similarity().stream(),
            applicationsFacade.similarity().write(),
            filteredKnnMutateStub,
            filteredNodeSimilarityMutateStub,
            knnMutateStub,
            nodeSimilarityMutateStub,
            configurationParser,
            user
        );
    }

    public FilteredKnnMutateStub filteredKnnMutateStub() {
        return filteredKnnMutateStub;
    }

    public Stream<KnnStatsResult> filteredKnnStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");
        var resultBuilder = new FilteredKnnResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return statsModeBusinessFacade.filteredKnn(
            GraphName.parse(graphName),
            parseConfiguration(configuration, FilteredKnnStatsConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> filteredKnnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.filteredKnn(
            parseConfiguration(algorithmConfiguration, FilteredKnnStatsConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public Stream<SimilarityResult> filteredKnnStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new FilteredKnnResultBuilderForStreamMode();

        return streamModeBusinessFacade.filteredKnn(
            GraphName.parse(graphName),
            parseConfiguration(configuration, FilteredKnnStreamConfig::of),
            resultBuilder
        );

    }

    public Stream<MemoryEstimateResult> filteredKnnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.filteredKnn(
            parseConfiguration(algorithmConfiguration, FilteredKnnStreamConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public Stream<KnnWriteResult> filteredKnnWrite(String graphNameAsString, Map<String, Object> rawConfiguration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");
        var resultBuilder = new FilteredKnnResultBuilderForWriteMode();

        return writeModeBusinessFacade.filteredKnn(
            GraphName.parse(graphNameAsString),
            parseConfiguration(rawConfiguration, FilteredKnnWriteConfig::of),
            resultBuilder,
            shouldComputeSimilarityDistribution
        );
    }

    public Stream<MemoryEstimateResult> filteredKnnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.filteredKnn(
            parseConfiguration(algorithmConfiguration, FilteredKnnWriteConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public FilteredNodeSimilarityMutateStub filteredNodeSimilarityMutateStub() {
        return filteredNodeSimilarityMutateStub;
    }

    public Stream<SimilarityStatsResult> filteredNodeSimilarityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");
        var resultBuilder = new FilteredNodeSimilarityResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return statsModeBusinessFacade.filteredNodeSimilarity(
            GraphName.parse(graphName),
            parseConfiguration(configuration, FilteredNodeSimilarityStatsConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> filteredNodeSimilarityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.filteredNodeSimilarity(
            parseConfiguration(algorithmConfiguration, FilteredNodeSimilarityStatsConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public Stream<SimilarityResult> filteredNodeSimilarityStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new FilteredNodeSimilarityResultBuilderForStreamMode();

        return streamModeBusinessFacade.filteredNodeSimilarity(
            GraphName.parse(graphName),
            parseConfiguration(configuration, FilteredNodeSimilarityStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> filteredNodeSimilarityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.filteredNodeSimilarity(
            parseConfiguration(algorithmConfiguration, FilteredNodeSimilarityStreamConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public Stream<SimilarityWriteResult> filteredNodeSimilarityWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new FilteredNodeSimilarityResultBuilderForWriteMode();

        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");

        return writeModeBusinessFacade.filteredNodeSimilarity(
            GraphName.parse(graphNameAsString),
            parseConfiguration(rawConfiguration, FilteredNodeSimilarityWriteConfig::of),
            resultBuilder,
            shouldComputeSimilarityDistribution
        );
    }

    public Stream<MemoryEstimateResult> filteredNodeSimilarityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.filteredNodeSimilarity(
            parseConfiguration(algorithmConfiguration, FilteredNodeSimilarityWriteConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public KnnMutateStub knnMutateStub() {
        return knnMutateStub;
    }

    public Stream<KnnStatsResult> knnStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");
        var resultBuilder = new KnnResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return statsModeBusinessFacade.knn(
            GraphName.parse(graphName),
            parseConfiguration(configuration, KnnStatsConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> knnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.knn(
            parseConfiguration(algorithmConfiguration, KnnStatsConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public Stream<SimilarityResult> knnStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KnnResultBuilderForStreamMode();

        return streamModeBusinessFacade.knn(
            GraphName.parse(graphName),
            parseConfiguration(configuration, KnnStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> knnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.knn(
            parseConfiguration(algorithmConfiguration, KnnStreamConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public Stream<KnnWriteResult> knnWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new KnnResultBuilderForWriteMode();

        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");

        return writeModeBusinessFacade.knn(
            GraphName.parse(graphNameAsString),
            parseConfiguration(rawConfiguration, KnnWriteConfig::of),
            resultBuilder,
            shouldComputeSimilarityDistribution
        );
    }

    public Stream<MemoryEstimateResult> knnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.knn(
            parseConfiguration(algorithmConfiguration, KnnWriteConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public NodeSimilarityMutateStub nodeSimilarityMutateStub() {
        return nodeSimilarityMutateStub;
    }

    public Stream<SimilarityStatsResult> nodeSimilarityStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");
        var resultBuilder = new NodeSimilarityResultBuilderForStatsMode(shouldComputeSimilarityDistribution);
        return statsModeBusinessFacade.nodeSimilarity(
            GraphName.parse(graphName),
            parseConfiguration(configuration, NodeSimilarityStatsConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> nodeSimilarityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.nodeSimilarity(
            parseConfiguration(algorithmConfiguration, NodeSimilarityStatsConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public Stream<SimilarityResult> nodeSimilarityStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new NodeSimilarityResultBuilderForStreamMode();

        return streamModeBusinessFacade.nodeSimilarity(
            GraphName.parse(graphName),
            parseConfiguration(configuration, NodeSimilarityStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> nodeSimilarityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeBusinessFacade.nodeSimilarity(
            parseConfiguration(algorithmConfiguration, NodeSimilarityStreamConfig::of),
            graphNameOrConfiguration
        );

        return Stream.of(result);
    }

    public Stream<SimilarityWriteResult> nodeSimilarityWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new NodeSimilarityResultBuilderForWriteMode();
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");

        var parsedConfiguration = parseConfiguration(rawConfiguration, NodeSimilarityWriteConfig::of);

        return writeModeBusinessFacade.nodeSimilarity(
            GraphName.parse(graphNameAsString),
            parsedConfiguration,
            resultBuilder,
            shouldComputeSimilarityDistribution
        );
    }

    public Stream<MemoryEstimateResult> nodeSimilarityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        var parsedConfiguration = parseConfiguration(algorithmConfiguration, NodeSimilarityWriteConfig::of);

        var memoryEstimateResult = estimationModeBusinessFacade.nodeSimilarity(
            parsedConfiguration,
            graphNameOrConfiguration
        );

        return Stream.of(memoryEstimateResult);
    }

    private <C extends AlgoBaseConfig> C parseConfiguration(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configurationMapper
    ) {
        return configurationParser.parseConfiguration(
            configuration,
            configurationMapper,
            user
        );
    }
}
