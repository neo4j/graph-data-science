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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.betweenness.BetweennessCentralityStatsConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStatsConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.closeness.ClosenessCentralityWriteConfig;
import org.neo4j.gds.degree.DegreeCentralityStatsConfig;
import org.neo4j.gds.degree.DegreeCentralityStreamConfig;
import org.neo4j.gds.degree.DegreeCentralityWriteConfig;
import org.neo4j.gds.harmonic.DeprecatedTieredHarmonicCentralityWriteConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStatsConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityWriteConfig;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.BetaClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.BetweennessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.ClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.DegreeCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.HarmonicCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
import org.neo4j.gds.procedures.algorithms.runners.StatsModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.runners.StreamModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.runners.WriteModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public final class CentralityProcedureFacade {
    private final ProcedureReturnColumns procedureReturnColumns;

    private final BetaClosenessCentralityMutateStub betaClosenessCentralityMutateStub;
    private final BetweennessCentralityMutateStub betweennessCentralityMutateStub;
    private final ClosenessCentralityMutateStub closenessCentralityMutateStub;
    private final DegreeCentralityMutateStub degreeCentralityMutateStub;

    private final HarmonicCentralityMutateStub harmonicCentralityMutateStub;
    private final ApplicationsFacade applicationsFacade;

    private final EstimationModeRunner estimationModeRunner;
    private final StatsModeAlgorithmRunner statsModeRunner;
    private final StreamModeAlgorithmRunner streamModeRunner;
    private final WriteModeAlgorithmRunner writeModeRunner;

    private CentralityProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        BetaClosenessCentralityMutateStub betaClosenessCentralityMutateStub,
        BetweennessCentralityMutateStub betweennessCentralityMutateStub,
        ClosenessCentralityMutateStub closenessCentralityMutateStub,
        DegreeCentralityMutateStub degreeCentralityMutateStub,
        HarmonicCentralityMutateStub harmonicCentralityMutateStub,
        ApplicationsFacade applicationsFacade,
        EstimationModeRunner estimationModeRunner,
        StatsModeAlgorithmRunner statsModeRunner,
        StreamModeAlgorithmRunner streamModeRunner,
        WriteModeAlgorithmRunner writeModeRunner
    ) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.betaClosenessCentralityMutateStub = betaClosenessCentralityMutateStub;
        this.betweennessCentralityMutateStub = betweennessCentralityMutateStub;
        this.closenessCentralityMutateStub = closenessCentralityMutateStub;
        this.degreeCentralityMutateStub = degreeCentralityMutateStub;
        this.harmonicCentralityMutateStub = harmonicCentralityMutateStub;
        this.applicationsFacade = applicationsFacade;
        this.estimationModeRunner = estimationModeRunner;
        this.statsModeRunner = statsModeRunner;
        this.streamModeRunner = streamModeRunner;
        this.writeModeRunner = writeModeRunner;
    }

    public static CentralityProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns,
        EstimationModeRunner estimationModeRunner,
        StatsModeAlgorithmRunner statsModeRunner,
        StreamModeAlgorithmRunner streamModeRunner,
        WriteModeAlgorithmRunner writeModeRunner
    ) {
        var betaClosenessCentralityMutateStub = new BetaClosenessCentralityMutateStub(
            genericStub,
            applicationsFacade,
            procedureReturnColumns
        );
        var betweennessCentralityMutateStub = new BetweennessCentralityMutateStub(
            genericStub,
            applicationsFacade,
            procedureReturnColumns
        );
        var closenessCentralityMutateStub = new ClosenessCentralityMutateStub(
            genericStub,
            applicationsFacade,
            procedureReturnColumns
        );
        var degreeCentralityMutateStub = new DegreeCentralityMutateStub(
            genericStub,
            applicationsFacade,
            procedureReturnColumns
        );
        var harmonicCentralityMutateStub = new HarmonicCentralityMutateStub(
            genericStub,
            applicationsFacade,
            procedureReturnColumns
        );

        return new CentralityProcedureFacade(
            procedureReturnColumns,
            betaClosenessCentralityMutateStub,
            betweennessCentralityMutateStub,
            closenessCentralityMutateStub,
            degreeCentralityMutateStub,
            harmonicCentralityMutateStub,
            applicationsFacade,
            estimationModeRunner,
            statsModeRunner,
            streamModeRunner,
            writeModeRunner
        );
    }

    public Stream<AlphaHarmonicStreamResult> alphaHarmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new AlphaHarmonicCentralityResultBuilderForStreamMode();

        return streamModeRunner.runStreamModeAlgorithm(
            graphName,
            configuration,
            HarmonicCentralityStreamConfig::of,
            resultBuilder,
            streamMode()::harmonicCentrality
        );
    }

    public BetaClosenessCentralityMutateStub betaClosenessCentralityMutateStub() {
        return betaClosenessCentralityMutateStub;
    }

    public Stream<BetaClosenessCentralityWriteResult> betaClosenessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new BetaClosenessCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        return writeModeRunner.runWriteModeAlgorithm(
            graphName,
            configuration,
            ClosenessCentralityWriteConfig::of,
            writeMode()::closenessCentrality,
            resultBuilder
        );
    }

    public BetweennessCentralityMutateStub betweennessCentralityMutateStub() {
        return betweennessCentralityMutateStub;
    }

    public Stream<CentralityStatsResult> betweennessCentralityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new BetweennessCentralityResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return statsModeRunner.runStatsModeAlgorithm(
            graphName,
            configuration,
            BetweennessCentralityStatsConfig::of,
            resultBuilder,
            statsMode()::betweennessCentrality
        );
    }

    public Stream<MemoryEstimateResult> betweennessCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            BetweennessCentralityStatsConfig::of,
            configuration -> estimationMode().betweennessCentrality(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<CentralityStreamResult> betweennessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new BetweennessCentralityResultBuilderForStreamMode();

        return streamModeRunner.runStreamModeAlgorithm(
            graphName,
            configuration,
            BetweennessCentralityStreamConfig::of,
            resultBuilder,
            streamMode()::betweennessCentrality
        );
    }

    public Stream<MemoryEstimateResult> betweennessCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            BetweennessCentralityStreamConfig::of,
            configuration -> estimationMode().betweennessCentrality(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<CentralityWriteResult> betweennessCentralityWrite(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new BetweennessCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        return writeModeRunner.runWriteModeAlgorithm(
            graphNameAsString,
            rawConfiguration,
            BetweennessCentralityWriteConfig::of,
            writeMode()::betweennessCentrality,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> betweennessCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            BetweennessCentralityWriteConfig::of,
            configuration -> estimationMode().betweennessCentrality(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public ClosenessCentralityMutateStub closenessCentralityMutateStub() {
        return closenessCentralityMutateStub;
    }

    public Stream<CentralityStatsResult> closenessCentralityStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new ClosenessCentralityResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return statsModeRunner.runStatsModeAlgorithm(
            graphName,
            configuration,
            ClosenessCentralityStatsConfig::of,
            resultBuilder,
            statsMode()::closenessCentrality
        );
    }

    public Stream<CentralityStreamResult> closenessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ClosenessCentralityResultBuilderForStreamMode();

        return streamModeRunner.runStreamModeAlgorithm(
            graphName,
            configuration,
            ClosenessCentralityStreamConfig::of,
            resultBuilder,
            streamMode()::closenessCentrality
        );
    }

    public Stream<CentralityWriteResult> closenessCentralityWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new ClosenessCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        return writeModeRunner.runWriteModeAlgorithm(
            graphName,
            configuration,
            ClosenessCentralityWriteConfig::of,
            writeMode()::closenessCentrality,
            resultBuilder
        );
    }

    public DegreeCentralityMutateStub degreeCentralityMutateStub() {
        return degreeCentralityMutateStub;
    }

    public Stream<CentralityStatsResult> degreeCentralityStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new DegreeCentralityResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return statsModeRunner.runStatsModeAlgorithm(
            graphName,
            configuration,
            DegreeCentralityStatsConfig::of,
            resultBuilder,
            statsMode()::degreeCentrality
        );
    }

    public Stream<MemoryEstimateResult> degreeCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            DegreeCentralityStatsConfig::of,
            configuration -> estimationMode().degreeCentrality(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<CentralityStreamResult> degreeCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new DegreeCentralityResultBuilderForStreamMode();

        return streamModeRunner.runStreamModeAlgorithm(
            graphName,
            configuration,
            DegreeCentralityStreamConfig::of,
            resultBuilder,
            streamMode()::degreeCentrality
        );
    }

    public Stream<MemoryEstimateResult> degreeCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            DegreeCentralityStreamConfig::of,
            configuration -> estimationMode().degreeCentrality(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<CentralityWriteResult> degreeCentralityWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new DegreeCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        return writeModeRunner.runWriteModeAlgorithm(
            graphName,
            configuration,
            DegreeCentralityWriteConfig::of,
            writeMode()::degreeCentrality,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> degreeCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationModeRunner.runEstimation(
            algorithmConfiguration,
            DegreeCentralityWriteConfig::of,
            configuration -> estimationMode().degreeCentrality(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public HarmonicCentralityMutateStub harmonicCentralityMutateStub() {
        return harmonicCentralityMutateStub;
    }

    public Stream<CentralityStatsResult> harmonicCentralityStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new HarmonicCentralityResultBuilderForStatsMode(shouldComputeSimilarityDistribution);

        return statsModeRunner.runStatsModeAlgorithm(
            graphName,
            configuration,
            HarmonicCentralityStatsConfig::of,
            resultBuilder,
            statsMode()::harmonicCentrality
        );
    }

    public Stream<CentralityStreamResult> harmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new HarmonicCentralityResultBuilderForStreamMode();

        return streamModeRunner.runStreamModeAlgorithm(
            graphName,
            configuration,
            HarmonicCentralityStreamConfig::of,
            resultBuilder,
            streamMode()::harmonicCentrality
        );
    }

    public Stream<CentralityWriteResult> harmonicCentralityWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new HarmonicCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        return writeModeRunner.runWriteModeAlgorithm(
            graphName,
            configuration,
            HarmonicCentralityWriteConfig::of,
            writeMode()::harmonicCentrality,
            resultBuilder
        );
    }

    public Stream<AlphaHarmonicWriteResult> alphaHarmonicCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new AlphaHarmonicCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        return writeModeRunner.runWriteModeAlgorithm(
            graphName,
            configuration,
            DeprecatedTieredHarmonicCentralityWriteConfig::of,
            writeMode()::harmonicCentrality,
            resultBuilder
        );
    }

    private CentralityAlgorithmsEstimationModeBusinessFacade estimationMode() {
        return applicationsFacade.centrality().estimate();
    }

    private CentralityAlgorithmsStatsModeBusinessFacade statsMode() {
        return applicationsFacade.centrality().stats();
    }

    private CentralityAlgorithmsStreamModeBusinessFacade streamMode() {
        return applicationsFacade.centrality().stream();
    }

    private CentralityAlgorithmsWriteModeBusinessFacade writeMode() {
        return applicationsFacade.centrality().write();
    }
}
