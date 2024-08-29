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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.procedures.algorithms.runners.AlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
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

    private final EstimationModeRunner estimationMode;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;

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
        EstimationModeRunner estimationMode,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding
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
        this.estimationMode = estimationMode;
        this.algorithmExecutionScaffolding = algorithmExecutionScaffolding;
    }

    public static SimilarityProcedureFacade create(
        ApplicationsFacade applicationsFacade,
        GenericStub genericStub,
        ProcedureReturnColumns procedureReturnColumns,
        EstimationModeRunner estimationModeRunner,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding)
    {
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
            estimationModeRunner,
            algorithmExecutionScaffolding
        );
    }

    public FilteredKnnMutateStub filteredKnnMutateStub() {
        return filteredKnnMutateStub;
    }

    public Stream<KnnStatsResult> filteredKnnStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");
        var resultBuilder = new FilteredKnnResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            FilteredKnnStatsConfig::of,
            statsModeBusinessFacade::filteredKnn,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> filteredKnnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            FilteredKnnStatsConfig::of,
            configuration -> estimationModeBusinessFacade.filteredKnn(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SimilarityResult> filteredKnnStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new FilteredKnnResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            FilteredKnnStreamConfig::of,
            streamModeBusinessFacade::filteredKnn,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> filteredKnnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            FilteredKnnStreamConfig::of,
            configuration -> estimationModeBusinessFacade.filteredKnn(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<KnnWriteResult> filteredKnnWrite(String graphNameAsString, Map<String, Object> rawConfiguration) {
        var resultBuilder = new FilteredKnnResultBuilderForWriteMode();

        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");

        return algorithmExecutionScaffolding.runAlgorithm(
            graphNameAsString,
            rawConfiguration,
            FilteredKnnWriteConfig::of,
            (graphName, configuration, __) -> writeModeBusinessFacade.filteredKnn(
                graphName,
                configuration,
                resultBuilder,
                shouldComputeSimilarityDistribution
            ),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> filteredKnnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            FilteredKnnWriteConfig::of,
            configuration -> estimationModeBusinessFacade.filteredKnn(
                configuration,
                graphNameOrConfiguration
            )
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            FilteredNodeSimilarityStatsConfig::of,
            statsModeBusinessFacade::filteredNodeSimilarity,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> filteredNodeSimilarityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            FilteredNodeSimilarityStatsConfig::of,
            configuration -> estimationModeBusinessFacade.filteredNodeSimilarity(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SimilarityResult> filteredNodeSimilarityStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new FilteredNodeSimilarityResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            FilteredNodeSimilarityStreamConfig::of,
            streamModeBusinessFacade::filteredNodeSimilarity,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> filteredNodeSimilarityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            FilteredNodeSimilarityStreamConfig::of,
            configuration -> estimationModeBusinessFacade.filteredNodeSimilarity(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SimilarityWriteResult> filteredNodeSimilarityWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new FilteredNodeSimilarityResultBuilderForWriteMode();

        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");

        return algorithmExecutionScaffolding.runAlgorithm(
            graphNameAsString,
            rawConfiguration,
            FilteredNodeSimilarityWriteConfig::of,
            (graphName, configuration, __) -> writeModeBusinessFacade.filteredNodeSimilarity(
                graphName,
                configuration,
                resultBuilder,
                shouldComputeSimilarityDistribution
            ),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> filteredNodeSimilarityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            FilteredNodeSimilarityWriteConfig::of,
            configuration -> estimationModeBusinessFacade.filteredNodeSimilarity(
                configuration,
                graphNameOrConfiguration
            )
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            KnnStatsConfig::of,
            statsModeBusinessFacade::knn,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> knnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            KnnStatsConfig::of,
            configuration -> estimationModeBusinessFacade.knn(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SimilarityResult> knnStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KnnResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            KnnStreamConfig::of,
            streamModeBusinessFacade::knn,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> knnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            KnnStreamConfig::of,
            configuration -> estimationModeBusinessFacade.knn(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<KnnWriteResult> knnWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new KnnResultBuilderForWriteMode();

        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");

        return algorithmExecutionScaffolding.runAlgorithm(
            graphNameAsString,
            rawConfiguration,
            KnnWriteConfig::of,
            (graphName, configuration, __) -> writeModeBusinessFacade.knn(
                graphName,
                configuration,
                resultBuilder,
                shouldComputeSimilarityDistribution
            ),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> knnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            KnnWriteConfig::of,
            configuration -> estimationModeBusinessFacade.knn(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public NodeSimilarityMutateStub nodeSimilarityMutateStub() {
        return nodeSimilarityMutateStub;
    }

    public Stream<SimilarityStatsResult> nodeSimilarityStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");
        var resultBuilder = new NodeSimilarityResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            NodeSimilarityStatsConfig::of,
            statsModeBusinessFacade::nodeSimilarity,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> nodeSimilarityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            NodeSimilarityStatsConfig::of,
            configuration -> estimationModeBusinessFacade.nodeSimilarity(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SimilarityResult> nodeSimilarityStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new NodeSimilarityResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            NodeSimilarityStreamConfig::of,
            streamModeBusinessFacade::nodeSimilarity,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> nodeSimilarityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            NodeSimilarityStreamConfig::of,
            configuration -> estimationModeBusinessFacade.nodeSimilarity(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SimilarityWriteResult> nodeSimilarityWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var resultBuilder = new NodeSimilarityResultBuilderForWriteMode();

        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");

        return algorithmExecutionScaffolding.runAlgorithm(
            graphNameAsString,
            rawConfiguration,
            NodeSimilarityWriteConfig::of,
            (graphName, configuration, __) -> writeModeBusinessFacade.nodeSimilarity(
                graphName,
                configuration,
                resultBuilder,
                shouldComputeSimilarityDistribution
            ),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> nodeSimilarityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            NodeSimilarityWriteConfig::of,
            configuration -> estimationModeBusinessFacade.nodeSimilarity(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

   
}
