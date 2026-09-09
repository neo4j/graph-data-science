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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityApplications;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.articulationpoints.ArticulationPointsStatsConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsStreamConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsWriteConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityStatsConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
import org.neo4j.gds.bridges.BridgesStreamConfig;
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
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStatsConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStreamConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationWriteConfig;
import org.neo4j.gds.pagerank.ArticleRankMutateConfig;
import org.neo4j.gds.pagerank.ArticleRankStatsConfig;
import org.neo4j.gds.pagerank.ArticleRankStreamConfig;
import org.neo4j.gds.pagerank.ArticleRankWriteConfig;
import org.neo4j.gds.pagerank.EigenvectorMutateConfig;
import org.neo4j.gds.pagerank.EigenvectorStatsConfig;
import org.neo4j.gds.pagerank.EigenvectorStreamConfig;
import org.neo4j.gds.pagerank.EigenvectorWriteConfig;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.pagerank.PageRankStatsConfig;
import org.neo4j.gds.pagerank.PageRankStreamConfig;
import org.neo4j.gds.pagerank.PageRankWriteConfig;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.CentralityStubs;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalArticulationPointsMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalBetaClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalBetweennessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalCelfMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalDegreeCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalHarmonicCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalHitsMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalPageRankMutateStub;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public final class LocalCentralityProcedureFacade implements CentralityProcedureFacade {
    private final ProcedureReturnColumns procedureReturnColumns;
    private final CentralityStubs stubs;
    private final UserSpecificConfigurationParser configurationParser;
    private final CentralityApplications centralityApplications;

    private LocalCentralityProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        CentralityStubs centralityStubs,
        UserSpecificConfigurationParser configurationParser,
        CentralityApplications centralityApplications
    ) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.stubs = centralityStubs;
        this.configurationParser = configurationParser;
        this.centralityApplications = centralityApplications;
    }

    public static CentralityProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns,
        UserSpecificConfigurationParser configurationParser
    ) {
        var articleRankMutateStub = new LocalPageRankMutateStub<>(
            genericStub,
            applicationsFacade.centrality().estimate(),
            procedureReturnColumns,
            applicationsFacade.centrality().mutate()::articleRank,
            ArticleRankMutateConfig::of
        );

        var articulationPointsMutateStub = new LocalArticulationPointsMutateStub(
            genericStub,
            applicationsFacade.centrality().mutate(),
            applicationsFacade.centrality().estimate()
        );

        var betaClosenessCentralityMutateStub = new LocalBetaClosenessCentralityMutateStub(
            genericStub,
            applicationsFacade.centrality().mutate(),
            applicationsFacade.centrality().estimate(),
            procedureReturnColumns
        );

        var betweennessCentralityMutateStub = new LocalBetweennessCentralityMutateStub(
            genericStub,
            applicationsFacade.centrality().mutate(),
            applicationsFacade.centrality().estimate(),
            procedureReturnColumns
        );

        var celfMutateStub = new LocalCelfMutateStub(
            genericStub,
            applicationsFacade.centrality().mutate(),
            applicationsFacade.centrality().estimate()
        );

        var closenessCentralityMutateStub = new LocalClosenessCentralityMutateStub(
            genericStub,
            applicationsFacade.centrality().mutate(),
            applicationsFacade.centrality().estimate(),
            procedureReturnColumns
        );

        var degreeCentralityMutateStub = new LocalDegreeCentralityMutateStub(
            genericStub,
            applicationsFacade.centrality().mutate(),
            applicationsFacade.centrality().estimate(),
            procedureReturnColumns
        );

        var eigenVectorMutateStub = new LocalPageRankMutateStub<>(
            genericStub,
            applicationsFacade.centrality().estimate(),
            procedureReturnColumns,
            applicationsFacade.centrality().mutate()::eigenVector,
            EigenvectorMutateConfig::of
        );

        var harmonicCentralityMutateStub = new LocalHarmonicCentralityMutateStub(
            genericStub,
            applicationsFacade.centrality().mutate(),
            applicationsFacade.centrality().estimate(),
            procedureReturnColumns
        );

        var hitsMutateStub = new LocalHitsMutateStub(
            genericStub,
            applicationsFacade.centrality().mutate(),
            applicationsFacade.centrality().estimate()
        );

        var pageRankMutateStub = new LocalPageRankMutateStub<>(
            genericStub,
            applicationsFacade.centrality().estimate(),
            procedureReturnColumns,
            applicationsFacade.centrality().mutate()::pageRank,
            PageRankMutateConfig::of
        );

        var stubs = new CentralityStubs(
            articleRankMutateStub,
            articulationPointsMutateStub,
            betaClosenessCentralityMutateStub,
            betweennessCentralityMutateStub,
            celfMutateStub,
            closenessCentralityMutateStub,
            degreeCentralityMutateStub,
            eigenVectorMutateStub,
            harmonicCentralityMutateStub,
            hitsMutateStub,
            pageRankMutateStub
        );

        return new LocalCentralityProcedureFacade(
            procedureReturnColumns,
            stubs,
            configurationParser,
            applicationsFacade.centrality()
        );
    }

    @Override
    public CentralityStubs centralityStubs() {
        return stubs;
    }

    @Override
    public Stream<AlphaHarmonicStreamResult> alphaHarmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new AlphaHarmonicCentralityResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HarmonicCentralityStreamConfig::of
        );

        return centralityApplications.stream().harmonicCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<AlphaHarmonicWriteResult> alphaHarmonicCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new AlphaHarmonicCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            DeprecatedTieredHarmonicCentralityWriteConfig::of
        );

        return centralityApplications.write().harmonicCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<PageRankMutateResult> articleRankMutate(String graphName, Map<String, Object> configuration) {
        return stubs.articleRank().execute(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> articleRankMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.articleRank().estimate(graphNameOrConfiguration, algorithmConfiguration);
    }

    @Override
    public Stream<PageRankStatsResult> articleRankStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ArticleRankStatsConfig::of
        );
        var resultBuilder = new PageRankResultBuilderForStatsMode<>(
            parsedConfiguration,
            shouldComputeSimilarityDistribution
        );

        return centralityApplications.stats().articleRank(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> articleRankStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ArticleRankStatsConfig::of
        );

        return Stream.of(centralityApplications.estimate().pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<CentralityStreamResult> articleRankStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PageRankResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ArticleRankStreamConfig::of
        );

        return centralityApplications.stream().articleRank(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> articleRankStreamEstimate(
        Object graphNameOrConfiguration, Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ArticleRankStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<PageRankWriteResult> articleRankWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new PageRankResultBuilderForWriteMode<ArticleRankWriteConfig>(
            shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ArticleRankWriteConfig::of
        );

        return centralityApplications.write().articleRank(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> articleRankWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ArticleRankWriteConfig::of
        );

        return Stream.of(centralityApplications.estimate().pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<ArticulationPointStreamResult> articulationPointsStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ArticulationPointsResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ArticulationPointsStreamConfig::of
        );

        return centralityApplications.stream().articulationPoints(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder,
            procedureReturnColumns.contains("resultingComponents")
        );
    }

    @Override
    public Stream<MemoryEstimateResult> articulationPointsStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ArticulationPointsStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().articulationPoints(
            parsedConfiguration,
            graphNameOrConfiguration,
            true
        ));
    }

    @Override
    public Stream<ArticulationPointsMutateResult> articulationPointsMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return stubs.articulationPoints().execute(graphName, configuration);

    }

    @Override
    public Stream<MemoryEstimateResult> articulationPointsMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.articulationPoints().estimate(graphNameOrConfiguration, algorithmConfiguration);
    }

    @Override
    public Stream<ArticulationPointsStatsResult> articulationPointsStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ArticulationPointsStatsConfig::of
        );

        var resultBuilder = new ArticulationPointsResultBuilderForStatsMode(parsedConfiguration);

        return centralityApplications.stats().articulationPoints(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> articulationPointsStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ArticulationPointsStatsConfig::of
        );

        return Stream.of(centralityApplications.estimate().articulationPoints(
            parsedConfiguration,
            graphNameOrConfiguration,
            false
        ));
    }

    @Override
    public Stream<ArticulationPointsWriteResult> articulationPointsWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ArticulationPointsWriteConfig::of
        );

        return centralityApplications.write().articulationPoints(
            GraphName.parse(graphName),
            parsedConfiguration,
            new ArticulationPointsResultBuilderForWriteMode()
        );
    }

    @Override
    public Stream<MemoryEstimateResult> articulationPointsWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ArticulationPointsWriteConfig::of
        );

        return Stream.of(centralityApplications.estimate().articulationPoints(
            parsedConfiguration,
            graphNameOrConfiguration,
            false
        ));
    }

    @Override
    public Stream<BetaClosenessCentralityMutateResult> betaClosenessCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return stubs.betaCloseness().execute(graphName, configuration);
    }


    @Override
    public Stream<BetaClosenessCentralityWriteResult> betaClosenessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new BetaClosenessCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ClosenessCentralityWriteConfig::of
        );

        return centralityApplications.write().closenessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<CentralityMutateResult> betweennessCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return stubs.betweenness().execute(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> betweennessCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.betweenness().estimate(graphNameOrConfiguration,algorithmConfiguration);
    }

    @Override
    public Stream<CentralityStatsResult> betweennessCentralityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            BetweennessCentralityStatsConfig::of
        );
        var resultBuilder = new BetweennessCentralityResultBuilderForStatsMode(
            parsedConfiguration,
            shouldComputeSimilarityDistribution
        );

        return centralityApplications.stats().betweennessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> betweennessCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            BetweennessCentralityStatsConfig::of
        );

        return Stream.of(centralityApplications.estimate().betweennessCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CentralityStreamResult> betweennessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new BetweennessCentralityResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            BetweennessCentralityStreamConfig::of
        );

        return centralityApplications.stream().betweennessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> betweennessCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            BetweennessCentralityStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().betweennessCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }


    @Override
    public Stream<CentralityWriteResult> betweennessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new BetweennessCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            BetweennessCentralityWriteConfig::of
        );

        return centralityApplications.write().betweennessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> betweennessCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            BetweennessCentralityWriteConfig::of
        );

        return Stream.of(centralityApplications.estimate().betweennessCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<BridgesStreamResult> bridgesStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var shouldComputeComponents = procedureReturnColumns.contains("remainingSizes");
        var resultBuilder = shouldComputeComponents
            ? new BridgesResultBuilderForStreamModeWithComponentSizes()
            : new BridgesResultBuilderForStreamModeWithoutComponentSizes();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            BridgesStreamConfig::of
        );

        return centralityApplications.stream().bridges(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder,
            shouldComputeComponents
        );
    }

    @Override
    public Stream<MemoryEstimateResult> bridgesStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            BridgesStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().bridges(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CELFMutateResult> celfMutate(String graphName, Map<String, Object> configuration) {
        return stubs.celf().execute(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> celfMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.celf().estimate(graphNameOrConfiguration, algorithmConfiguration);
    }

    @Override
    public Stream<CELFStatsResult> celfStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            InfluenceMaximizationStatsConfig::of
        );
        var resultBuilder = new CelfResultBuilderForStatsMode(parsedConfiguration);

        return centralityApplications.stats().celf(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> celfStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            InfluenceMaximizationStatsConfig::of
        );

        return Stream.of(centralityApplications.estimate().celf(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CELFStreamResult> celfStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new CelfResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            InfluenceMaximizationStreamConfig::of
        );

        return centralityApplications.stream().celf(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> celfStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            InfluenceMaximizationStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().celf(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CELFWriteResult> celfWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new CelfResultBuilderForWriteMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            InfluenceMaximizationWriteConfig::of
        );

        return centralityApplications.write().celf(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> celfWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            InfluenceMaximizationWriteConfig::of
        );

        return Stream.of(centralityApplications.estimate().celf(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CentralityMutateResult> closenessCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return stubs.closeness().execute(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> closenessCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.closeness().estimate(graphNameOrConfiguration, algorithmConfiguration);
    }

    @Override
    public Stream<CentralityStatsResult> closenessCentralityStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ClosenessCentralityStatsConfig::of
        );
        var resultBuilder = new ClosenessCentralityResultBuilderForStatsMode(
            parsedConfiguration,
            shouldComputeSimilarityDistribution
        );

        return centralityApplications.stats().closenessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> closenessCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ClosenessCentralityStatsConfig::of
        );

        return Stream.of(centralityApplications.estimate().closenessCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CentralityStreamResult> closenessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ClosenessCentralityResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ClosenessCentralityStreamConfig::of
        );

        return centralityApplications.stream().closenessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> closenessCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ClosenessCentralityStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().closenessCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CentralityWriteResult> closenessCentralityWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new ClosenessCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ClosenessCentralityWriteConfig::of
        );

        return centralityApplications.write().closenessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> closenessCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ClosenessCentralityWriteConfig::of
        );

        return Stream.of(centralityApplications.estimate().closenessCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CentralityMutateResult> degreeCentralityMutate(String graphName, Map<String, Object> configuration) {
        return stubs.degree().execute(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> degreeCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.degree().estimate(graphNameOrConfiguration, algorithmConfiguration);

    }

    @Override
    public Stream<CentralityStatsResult> degreeCentralityStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            DegreeCentralityStatsConfig::of
        );
        var resultBuilder = new DegreeCentralityResultBuilderForStatsMode(
            parsedConfiguration,
            shouldComputeSimilarityDistribution
        );

        return centralityApplications.stats().degreeCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> degreeCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            DegreeCentralityStatsConfig::of
        );

        return Stream.of(centralityApplications.estimate().degreeCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CentralityStreamResult> degreeCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new DegreeCentralityResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            DegreeCentralityStreamConfig::of
        );

        return centralityApplications.stream().degreeCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> degreeCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            DegreeCentralityStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().degreeCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CentralityWriteResult> degreeCentralityWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new DegreeCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            DegreeCentralityWriteConfig::of
        );

        return centralityApplications.write().degreeCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> degreeCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            DegreeCentralityWriteConfig::of
        );

        return Stream.of(centralityApplications.estimate().degreeCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<PageRankMutateResult> eigenvectorMutate(String graphName, Map<String, Object> configuration) {
        return stubs.eigenvector().execute(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> eigenvectorMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.eigenvector().estimate(graphNameOrConfiguration, algorithmConfiguration);
    }

    @Override
    public Stream<PageRankStatsResult> eigenvectorStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");


        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            EigenvectorStatsConfig::of
        );
        var resultBuilder = new PageRankResultBuilderForStatsMode<>(
            parsedConfiguration,
            shouldComputeSimilarityDistribution
        );

        return centralityApplications.stats().eigenVector(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> eigenvectorStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            EigenvectorStatsConfig::of
        );

        return Stream.of(centralityApplications.estimate().pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<CentralityStreamResult> eigenvectorStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PageRankResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            EigenvectorStreamConfig::of
        );

        return centralityApplications.stream().eigenvector(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> eigenvectorStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            EigenvectorStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<PageRankWriteResult> eigenvectorWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new PageRankResultBuilderForWriteMode<EigenvectorWriteConfig>(
            shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            EigenvectorWriteConfig::of
        );

        return centralityApplications.write().eigenvector(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> eigenvectorWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            EigenvectorWriteConfig::of
        );

        return Stream.of(centralityApplications.estimate().pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<CentralityMutateResult> harmonicCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return stubs.harmonic().execute(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> harmonicCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.harmonic().estimate(graphNameOrConfiguration, algorithmConfiguration);
    }

    @Override
    public Stream<CentralityStatsResult> harmonicCentralityStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HarmonicCentralityStatsConfig::of
        );

        var resultBuilder = new HarmonicCentralityResultBuilderForStatsMode(
            parsedConfiguration,
            shouldComputeSimilarityDistribution
        );

        return centralityApplications.stats().harmonicCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> harmonicCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            HarmonicCentralityStatsConfig::of
        );

        return Stream.of(centralityApplications.estimate().harmonicCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CentralityStreamResult> harmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new HarmonicCentralityResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HarmonicCentralityStreamConfig::of
        );

        return centralityApplications.stream().harmonicCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> harmonicCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            HarmonicCentralityStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().harmonicCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<CentralityWriteResult> harmonicCentralityWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new HarmonicCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HarmonicCentralityWriteConfig::of
        );

        return centralityApplications.write().harmonicCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> harmonicCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            HarmonicCentralityWriteConfig::of
        );

        return Stream.of(centralityApplications.estimate().harmonicCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public Stream<HitsStreamResult> hitsStream(String graphName, Map<String, Object> configuration) {

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HitsConfig::of
        );

        var resultBuilder = new HitsResultBuilderForStreamMode(parsedConfiguration);

        return centralityApplications.stream().hits(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> hitsStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            HitsConfig::of
        );
        return Stream.of(centralityApplications.estimate().hits(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<HitsStatsResult> hitsStats(String graphName, Map<String, Object> configuration) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HitsConfig::of
        );

        var resultBuilder = new HitsResultBuilderForStatsMode(parsedConfiguration);

        return centralityApplications.stats().hits(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> hitsStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            HitsConfig::of
        );
        return Stream.of(centralityApplications.estimate().hits(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<HitsWriteResult> hitsWrite(String graphName, Map<String, Object> configuration) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HitsConfig::of
        );

        var resultBuilder = new HitsResultBuilderForWriteMode();

        return centralityApplications.write().hits(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> hitsWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            HitsConfig::of
        );
        return Stream.of(centralityApplications.estimate().hits(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<HitsMutateResult> hitsMutate(String graphName, Map<String, Object> configuration) {
        return stubs.hits().execute(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> hitsMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.hits().estimate(graphNameOrConfiguration, algorithmConfiguration);

    }

    @Override
    public Stream<PageRankMutateResult> pageRankMutate(String graphName, Map<String, Object> configuration) {
        return stubs.pageRank().execute(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> pageRankMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return stubs.pageRank().estimate(graphNameOrConfiguration, algorithmConfiguration);
    }

    @Override
    public Stream<PageRankStatsResult> pageRankStats(String graphName, Map<String, Object> configuration) {
        var shouldComputeSimilarityDistribution = procedureReturnColumns.contains("centralityDistribution");

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            PageRankStatsConfig::of
        );
        var resultBuilder = new PageRankResultBuilderForStatsMode<>(
            parsedConfiguration,
            shouldComputeSimilarityDistribution
        );

        return centralityApplications.stats().pageRank(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> pageRankStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            PageRankStatsConfig::of
        );

        return Stream.of(centralityApplications.estimate().pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<CentralityStreamResult> pageRankStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PageRankResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            PageRankStreamConfig::of
        );

        return centralityApplications.stream().pageRank(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> pageRankStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            PageRankStreamConfig::of
        );

        return Stream.of(centralityApplications.estimate().pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<PageRankWriteResult> pageRankWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new PageRankResultBuilderForWriteMode<PageRankWriteConfig>(
            shouldComputeCentralityDistribution);


        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            PageRankWriteConfig::of
        );

        return centralityApplications.write().pageRank(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<MemoryEstimateResult> pageRankWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            PageRankWriteConfig::of
        );

        return Stream.of(centralityApplications.estimate().pageRank(parsedConfiguration, graphNameOrConfiguration));
    }
}
