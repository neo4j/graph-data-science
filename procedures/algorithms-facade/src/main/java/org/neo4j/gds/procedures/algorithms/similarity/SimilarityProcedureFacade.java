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
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.runners.StatsModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.runners.StreamModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.runners.WriteModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStatsConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStreamConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnWriteConfig;
import org.neo4j.gds.similarity.knn.KnnStatsConfig;
import org.neo4j.gds.similarity.knn.KnnStreamConfig;
import org.neo4j.gds.similarity.knn.KnnWriteConfig;

import java.util.Map;
import java.util.stream.Stream;

public final class SimilarityProcedureFacade {
    private final ProcedureReturnColumns procedureReturnColumns;
    private final FilteredKnnMutateStub filteredKnnMutateStub;
    private final KnnMutateStub knnMutateStub;
    private final ApplicationsFacade applicationsFacade;
    private final EstimationModeRunner estimationModeRunner;
    private final StreamModeAlgorithmRunner streamModeAlgorithmRunner;
    private final StatsModeAlgorithmRunner statsModeAlgorithmRunner;
    private final WriteModeAlgorithmRunner writeModeAlgorithmRunner;

    private SimilarityProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        FilteredKnnMutateStub filteredKnnMutateStub,
        KnnMutateStub knnMutateStub,
        ApplicationsFacade applicationsFacade,
        EstimationModeRunner estimationModeRunner,
        StreamModeAlgorithmRunner streamModeAlgorithmRunner,
        StatsModeAlgorithmRunner statsModeAlgorithmRunner,
        WriteModeAlgorithmRunner writeModeAlgorithmRunner
    ) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.filteredKnnMutateStub = filteredKnnMutateStub;
        this.knnMutateStub = knnMutateStub;
        this.applicationsFacade = applicationsFacade;
        this.estimationModeRunner = estimationModeRunner;
        this.streamModeAlgorithmRunner = streamModeAlgorithmRunner;
        this.statsModeAlgorithmRunner = statsModeAlgorithmRunner;
        this.writeModeAlgorithmRunner = writeModeAlgorithmRunner;
    }

    public static SimilarityProcedureFacade create(
        ApplicationsFacade applicationsFacade,
        GenericStub genericStub,
        ProcedureReturnColumns procedureReturnColumns,
        EstimationModeRunner estimationModeRunner,
        StreamModeAlgorithmRunner streamModeAlgorithmRunner,
        StatsModeAlgorithmRunner statsModeAlgorithmRunner,
        WriteModeAlgorithmRunner writeModeAlgorithmRunner
    ) {
        var filteredKnnMutateStub = new FilteredKnnMutateStub(genericStub, applicationsFacade, procedureReturnColumns);
        var knnMutateStub = new KnnMutateStub(genericStub, applicationsFacade, procedureReturnColumns);

        return new SimilarityProcedureFacade(
            procedureReturnColumns,
            filteredKnnMutateStub,
            knnMutateStub,
            applicationsFacade,
            estimationModeRunner,
            streamModeAlgorithmRunner,
            statsModeAlgorithmRunner,
            writeModeAlgorithmRunner
        );
    }

    public FilteredKnnMutateStub filteredKnnMutateStub() {
        return filteredKnnMutateStub;
    }

    public Stream<KnnStatsResult> filteredKnnStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");
        var resultBuilder = new FilteredKnnResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return statsModeAlgorithmRunner.runStatsModeAlgorithm(
            graphName,
            configuration,
            FilteredKnnStatsConfig::of,
            resultBuilder,
            statsMode()::filteredKnn
        );
    }

    public Stream<MemoryEstimateResult> filteredKnnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            FilteredKnnStatsConfig::of,
            configuration -> estimationMode().filteredKnn(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SimilarityResult> filteredKnnStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new FilteredKnnResultBuilderForStreamMode();

        return streamModeAlgorithmRunner.runStreamModeAlgorithm(
            graphName,
            configuration,
            FilteredKnnStreamConfig::of,
            resultBuilder,
            streamMode()::filteredKnn
        );
    }

    public Stream<MemoryEstimateResult> filteredKnnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            FilteredKnnStreamConfig::of,
            configuration -> estimationMode().filteredKnn(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<KnnWriteResult> filteredKnnWrite(String graphNameAsString, Map<String, Object> rawConfiguration) {
        var resultBuilder = new FilteredKnnResultBuilderForWriteMode();

        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("similarityDistribution");

        return writeModeAlgorithmRunner.runWriteModeAlgorithm(
            graphNameAsString,
            rawConfiguration,
            FilteredKnnWriteConfig::of,
            (graphName, configuration, __) -> writeMode().filteredKnn(
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
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            FilteredKnnWriteConfig::of,
            configuration -> estimationMode().filteredKnn(
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

        return statsModeAlgorithmRunner.runStatsModeAlgorithm(
            graphName,
            configuration,
            KnnStatsConfig::of,
            resultBuilder,
            statsMode()::knn
        );
    }

    public Stream<MemoryEstimateResult> knnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            KnnStatsConfig::of,
            configuration -> estimationMode().knn(
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

        return streamModeAlgorithmRunner.runStreamModeAlgorithm(
            graphName,
            configuration,
            KnnStreamConfig::of,
            resultBuilder,
            streamMode()::knn
        );
    }

    public Stream<MemoryEstimateResult> knnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            KnnStreamConfig::of,
            configuration -> estimationMode().knn(
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

        return writeModeAlgorithmRunner.runWriteModeAlgorithm(
            graphNameAsString,
            rawConfiguration,
            KnnWriteConfig::of,
            (graphName, configuration, __) -> writeMode().knn(
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
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            KnnWriteConfig::of,
            configuration -> estimationMode().knn(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    private SimilarityAlgorithmsEstimationModeBusinessFacade estimationMode() {
        return applicationsFacade.similarity().estimate();
    }

    private SimilarityAlgorithmsStatsModeBusinessFacade statsMode() {
        return applicationsFacade.similarity().stats();
    }

    private SimilarityAlgorithmsStreamModeBusinessFacade streamMode() {
        return applicationsFacade.similarity().stream();
    }

    private SimilarityAlgorithmsWriteModeBusinessFacade writeMode() {
        return applicationsFacade.similarity().write();
    }
}
