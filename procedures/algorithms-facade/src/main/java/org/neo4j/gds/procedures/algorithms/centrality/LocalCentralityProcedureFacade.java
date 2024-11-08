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
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.articulationpoints.ArticulationPointsMutateConfig;
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
import org.neo4j.gds.procedures.algorithms.centrality.stubs.ArticulationPointsMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.BetaClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.BetweennessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.CelfMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.ClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.DegreeCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.HarmonicCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.HitsMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalArticulationPointsMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalBetaClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalBetweennessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalCelfMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalDegreeCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalHarmonicCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalHitsMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.LocalPageRankMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.PageRankMutateStub;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;

import java.util.Map;
import java.util.stream.Stream;

public final class LocalCentralityProcedureFacade implements CentralityProcedureFacade {
    private final ProcedureReturnColumns procedureReturnColumns;

    private final ArticulationPointsMutateStub articulationPointsMutateStub;
    private final PageRankMutateStub<ArticleRankMutateConfig> articleRankMutateStub;
    private final BetaClosenessCentralityMutateStub betaClosenessCentralityMutateStub;
    private final BetweennessCentralityMutateStub betweennessCentralityMutateStub;
    private final CelfMutateStub celfMutateStub;
    private final HitsMutateStub hitsMutateStub;

    private final ClosenessCentralityMutateStub closenessCentralityMutateStub;
    private final DegreeCentralityMutateStub degreeCentralityMutateStub;
    private final PageRankMutateStub<EigenvectorMutateConfig> eigenVectorMutateStub;
    private final HarmonicCentralityMutateStub harmonicCentralityMutateStub;
    private final PageRankMutateStub<PageRankMutateConfig> pageRankMutateStub;

    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final CentralityAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade;
    private final CentralityAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade;
    private final CentralityAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade;

    private final UserSpecificConfigurationParser configurationParser;

    private LocalCentralityProcedureFacade(
        ProcedureReturnColumns procedureReturnColumns,
        PageRankMutateStub<ArticleRankMutateConfig> articleRankMutateStub,
        BetaClosenessCentralityMutateStub betaClosenessCentralityMutateStub,
        BetweennessCentralityMutateStub betweennessCentralityMutateStub,
        CelfMutateStub celfMutateStub,
        DegreeCentralityMutateStub degreeCentralityMutateStub,
        ClosenessCentralityMutateStub closenessCentralityMutateStub,
        HitsMutateStub hitsMutateStub,
        PageRankMutateStub<EigenvectorMutateConfig> eigenVectorMutateStub,
        HarmonicCentralityMutateStub harmonicCentralityMutateStub,
        ArticulationPointsMutateStub articulationPointsMutateStub,
        PageRankMutateStub<PageRankMutateConfig> pageRankMutateStub,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        CentralityAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade,
        CentralityAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade,
        CentralityAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade,
        UserSpecificConfigurationParser configurationParser
    ) {
        this.procedureReturnColumns = procedureReturnColumns;
        this.articleRankMutateStub = articleRankMutateStub;
        this.hitsMutateStub = hitsMutateStub;
        this.articulationPointsMutateStub = articulationPointsMutateStub;
        this.betaClosenessCentralityMutateStub = betaClosenessCentralityMutateStub;
        this.betweennessCentralityMutateStub = betweennessCentralityMutateStub;
        this.celfMutateStub = celfMutateStub;
        this.closenessCentralityMutateStub = closenessCentralityMutateStub;
        this.degreeCentralityMutateStub = degreeCentralityMutateStub;
        this.eigenVectorMutateStub = eigenVectorMutateStub;
        this.harmonicCentralityMutateStub = harmonicCentralityMutateStub;
        this.pageRankMutateStub = pageRankMutateStub;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.statsModeBusinessFacade = statsModeBusinessFacade;
        this.streamModeBusinessFacade = streamModeBusinessFacade;
        this.writeModeBusinessFacade = writeModeBusinessFacade;
        this.configurationParser = configurationParser;
    }

    public static CentralityProcedureFacade create(
        GenericStub genericStub,
        ApplicationsFacade applicationsFacade,
        ProcedureReturnColumns procedureReturnColumns,
        UserSpecificConfigurationParser configurationParser
    ) {
        var centralityApplications = applicationsFacade.centrality();
        var mutateModeBusinessFacade = centralityApplications.mutate();
        var estimationModeBusinessFacade = centralityApplications.estimate();

        var articleRankMutateStub = new LocalPageRankMutateStub<>(
            genericStub,
            estimationModeBusinessFacade,
            procedureReturnColumns,
            mutateModeBusinessFacade::articleRank,
            ArticleRankMutateConfig::of
        );

        var betaClosenessCentralityMutateStub = new LocalBetaClosenessCentralityMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade,
            procedureReturnColumns
        );

        var betweennessCentralityMutateStub = new LocalBetweennessCentralityMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade,
            procedureReturnColumns
        );

        var celfMutateStub = new LocalCelfMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var closenessCentralityMutateStub = new LocalClosenessCentralityMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade,
            procedureReturnColumns
        );

        var degreeCentralityMutateStub = new LocalDegreeCentralityMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade,
            procedureReturnColumns
        );

        var eigenVectorMutateStub = new LocalPageRankMutateStub<>(
            genericStub,
            estimationModeBusinessFacade,
            procedureReturnColumns,
            mutateModeBusinessFacade::eigenVector,
            EigenvectorMutateConfig::of
        );

        var harmonicCentralityMutateStub = new LocalHarmonicCentralityMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade,
            procedureReturnColumns
        );

        var pageRankMutateStub = new LocalPageRankMutateStub<>(
            genericStub,
            estimationModeBusinessFacade,
            procedureReturnColumns,
            mutateModeBusinessFacade::pageRank,
            PageRankMutateConfig::of

        );

        var articulationPointsMutateStub = new LocalArticulationPointsMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var hitsMutateStub = new LocalHitsMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        return new LocalCentralityProcedureFacade(
            procedureReturnColumns,
            articleRankMutateStub,
            betaClosenessCentralityMutateStub,
            betweennessCentralityMutateStub,
            celfMutateStub,
            degreeCentralityMutateStub, closenessCentralityMutateStub, hitsMutateStub,
            eigenVectorMutateStub,
            harmonicCentralityMutateStub,
            articulationPointsMutateStub,
            pageRankMutateStub,
            estimationModeBusinessFacade,
            centralityApplications.stats(),
            centralityApplications.stream(),
            centralityApplications.write(),
            configurationParser
        );
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

        return streamModeBusinessFacade.harmonicCentrality(
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

        return writeModeBusinessFacade.harmonicCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public PageRankMutateStub<ArticleRankMutateConfig> articleRankMutateStub() {
        return articleRankMutateStub;
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

        return statsModeBusinessFacade.articleRank(
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

        return Stream.of(estimationModeBusinessFacade.pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<CentralityStreamResult> articleRankStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PageRankResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ArticleRankStreamConfig::of
        );

        return streamModeBusinessFacade.articleRank(
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

        return Stream.of(estimationModeBusinessFacade.pageRank(parsedConfiguration, graphNameOrConfiguration));
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

        return writeModeBusinessFacade.articleRank(
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

        return Stream.of(estimationModeBusinessFacade.pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public BetaClosenessCentralityMutateStub betaClosenessCentralityMutateStub() {
        return betaClosenessCentralityMutateStub;
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

        return writeModeBusinessFacade.closenessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public BetweennessCentralityMutateStub betweennessCentralityMutateStub() {
        return betweennessCentralityMutateStub;
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

        return statsModeBusinessFacade.betweennessCentrality(
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

        return Stream.of(estimationModeBusinessFacade.betweennessCentrality(
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

        return streamModeBusinessFacade.betweennessCentrality(
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

        return Stream.of(estimationModeBusinessFacade.betweennessCentrality(
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

        return writeModeBusinessFacade.betweennessCentrality(
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

        return Stream.of(estimationModeBusinessFacade.betweennessCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }


    @Override
    public Stream<ArticulationPoint> articulationPointsStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new ArticulationPointsResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ArticulationPointsStreamConfig::of
        );

        return streamModeBusinessFacade.articulationPoints(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
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

        return Stream.of(estimationModeBusinessFacade.articulationPoints(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public ArticulationPointsMutateStub articulationPointsMutateStub() {return articulationPointsMutateStub;}

    @Override
    public Stream<MemoryEstimateResult> articulationPointsMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            algorithmConfiguration,
            ArticulationPointsMutateConfig::of
        );

        return Stream.of(estimationModeBusinessFacade.articulationPoints(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
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

        return statsModeBusinessFacade.articulationPoints(
            GraphName.parse(graphName),
            parsedConfiguration,
            new ArticulationPointsResultBuilderForStatsMode(parsedConfiguration)
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

        return Stream.of(estimationModeBusinessFacade.articulationPoints(
            parsedConfiguration,
            graphNameOrConfiguration
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

        return writeModeBusinessFacade.articulationPoints(
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

        return Stream.of(estimationModeBusinessFacade.articulationPoints(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }


    @Override
    public Stream<BridgesStreamResult> bridgesStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new BridgesResultBuilderForStreamMode();
        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            BridgesStreamConfig::of
        );

        return streamModeBusinessFacade.bridges(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
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

        return Stream.of(estimationModeBusinessFacade.bridges(
            parsedConfiguration,
            graphNameOrConfiguration
        ));

    }

    @Override
    public CelfMutateStub celfMutateStub() {
        return celfMutateStub;
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

        return statsModeBusinessFacade.celf(
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

        return Stream.of(estimationModeBusinessFacade.celf(
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

        return streamModeBusinessFacade.celf(
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

        return Stream.of(estimationModeBusinessFacade.celf(
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

        return writeModeBusinessFacade.celf(
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

        return Stream.of(estimationModeBusinessFacade.celf(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public ClosenessCentralityMutateStub closenessCentralityMutateStub() {
        return closenessCentralityMutateStub;
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

        return statsModeBusinessFacade.closenessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
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

        return streamModeBusinessFacade.closenessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<CentralityWriteResult> closenessCentralityWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new ClosenessCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            ClosenessCentralityWriteConfig::of
        );

        return writeModeBusinessFacade.closenessCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public DegreeCentralityMutateStub degreeCentralityMutateStub() {
        return degreeCentralityMutateStub;
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

        return statsModeBusinessFacade.degreeCentrality(
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

        return Stream.of(estimationModeBusinessFacade.degreeCentrality(
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

        return streamModeBusinessFacade.degreeCentrality(
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

        return Stream.of(estimationModeBusinessFacade.degreeCentrality(
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

        return writeModeBusinessFacade.degreeCentrality(
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

        return Stream.of(estimationModeBusinessFacade.degreeCentrality(
            parsedConfiguration,
            graphNameOrConfiguration
        ));
    }

    @Override
    public PageRankMutateStub<EigenvectorMutateConfig> eigenVectorMutateStub() {
        return eigenVectorMutateStub;
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

        return statsModeBusinessFacade.eigenVector(
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

        return Stream.of(estimationModeBusinessFacade.pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<CentralityStreamResult> eigenvectorStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PageRankResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            EigenvectorStreamConfig::of
        );

        return streamModeBusinessFacade.eigenvector(
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

        return Stream.of(estimationModeBusinessFacade.pageRank(parsedConfiguration, graphNameOrConfiguration));
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

        return writeModeBusinessFacade.eigenvector(
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

        return Stream.of(estimationModeBusinessFacade.pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public HarmonicCentralityMutateStub harmonicCentralityMutateStub() {
        return harmonicCentralityMutateStub;
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

        return statsModeBusinessFacade.harmonicCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
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

        return streamModeBusinessFacade.harmonicCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public Stream<CentralityWriteResult> harmonicCentralityWrite(String graphName, Map<String, Object> configuration) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new HarmonicCentralityResultBuilderForWriteMode(shouldComputeCentralityDistribution);

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HarmonicCentralityWriteConfig::of
        );

        return writeModeBusinessFacade.harmonicCentrality(
            GraphName.parse(graphName),
            parsedConfiguration,
            resultBuilder
        );
    }

    @Override
    public PageRankMutateStub<PageRankMutateConfig> pageRankMutateStub() {
        return pageRankMutateStub;
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

        return statsModeBusinessFacade.pageRank(
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

        return Stream.of(estimationModeBusinessFacade.pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<CentralityStreamResult> pageRankStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PageRankResultBuilderForStreamMode();

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            PageRankStreamConfig::of
        );

        return streamModeBusinessFacade.pageRank(
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

        return Stream.of(estimationModeBusinessFacade.pageRank(parsedConfiguration, graphNameOrConfiguration));
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

        return writeModeBusinessFacade.pageRank(
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

        return Stream.of(estimationModeBusinessFacade.pageRank(parsedConfiguration, graphNameOrConfiguration));
    }

    @Override
    public Stream<HitsStreamResult> hitsStream(String graphName, Map<String, Object> configuration) {

        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HitsConfig::of
        );

        var resultBuilder =new HitsResultBuilderForStreamMode(parsedConfiguration);

        return streamModeBusinessFacade.hits(
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
        return Stream.of(estimationModeBusinessFacade.hits(parsedConfiguration,graphNameOrConfiguration));
    }

    @Override
    public Stream<HitsStatsResult> hitsStats(String graphName, Map<String, Object> configuration) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HitsConfig::of
        );

        var resultBuilder =new HitsResultBuilderForStatsMode(parsedConfiguration);

        return statsModeBusinessFacade.hits(
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
        return Stream.of(estimationModeBusinessFacade.hits(parsedConfiguration,graphNameOrConfiguration));
    }

    @Override
    public Stream<HitsWriteResult> hitsWrite(String graphName, Map<String, Object> configuration) {
        var parsedConfiguration = configurationParser.parseConfiguration(
            configuration,
            HitsConfig::of
        );

        var resultBuilder =new HitsResultBuilderForWriteMode();

        return writeModeBusinessFacade.hits(
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
        return Stream.of(estimationModeBusinessFacade.hits(parsedConfiguration,graphNameOrConfiguration));
    }

    @Override
    public HitsMutateStub hitsMutateStub() {
        return hitsMutateStub;
    }

}
