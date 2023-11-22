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
package org.neo4j.gds.procedures.centrality;

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityStatsConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
import org.neo4j.gds.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStatsConfig;
import org.neo4j.gds.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.closeness.ClosenessCentralityWriteConfig;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.degree.DegreeCentralityMutateConfig;
import org.neo4j.gds.degree.DegreeCentralityStatsConfig;
import org.neo4j.gds.degree.DegreeCentralityStreamConfig;
import org.neo4j.gds.degree.DegreeCentralityWriteConfig;
import org.neo4j.gds.harmonic.DeprecatedTieredHarmonicCentralityWriteConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityMutateConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStatsConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityWriteConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStreamConfig;
import org.neo4j.gds.procedures.centrality.alphaharmonic.AlphaHarmonicStreamResult;
import org.neo4j.gds.procedures.centrality.alphaharmonic.AlphaHarmonicWriteResult;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.pagerank.PageRankStatsConfig;
import org.neo4j.gds.pagerank.PageRankStreamConfig;
import org.neo4j.gds.pagerank.PageRankWriteConfig;
import org.neo4j.gds.procedures.centrality.betacloseness.BetaClosenessCentralityMutateResult;
import org.neo4j.gds.procedures.centrality.betacloseness.BetaClosenessCentralityWriteResult;
import org.neo4j.gds.procedures.centrality.celf.CELFStreamResult;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankComputationalResultTransformer;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankMutateResult;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankStatsResult;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankWriteResult;
import org.neo4j.gds.procedures.configparser.ConfigurationParser;
import org.neo4j.gds.results.MemoryEstimateResult;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class CentralityProcedureFacade {

    private final ConfigurationParser configurationParser;
    private final User user;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final CentralityAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final CentralityAlgorithmsStatsBusinessFacade statsBusinessFacade;
    private final CentralityAlgorithmsStreamBusinessFacade streamBusinessFacade;
    private final CentralityAlgorithmsWriteBusinessFacade writeBusinessFacade;

    private final CentralityAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final AlgorithmMetaDataSetter algorithmMetaDataSetter;

    public CentralityProcedureFacade(
        ConfigurationParser configurationParser,
        User user,
        ProcedureReturnColumns procedureReturnColumns,
        CentralityAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        CentralityAlgorithmsStatsBusinessFacade statsBusinessFacade,
        CentralityAlgorithmsStreamBusinessFacade streamBusinessFacade,
        CentralityAlgorithmsWriteBusinessFacade writeBusinessFacade,
        CentralityAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        this.configurationParser = configurationParser;
        this.user = user;
        this.procedureReturnColumns = procedureReturnColumns;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.statsBusinessFacade = statsBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
        this.writeBusinessFacade = writeBusinessFacade;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.algorithmMetaDataSetter = algorithmMetaDataSetter;
    }

    public Stream<CentralityStreamResult> betweenessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createStreamConfig(configuration, BetweennessCentralityStreamConfig::of);

        var computationResult = streamBusinessFacade.betweennessCentrality(
            graphName,
            config
        );

        return DefaultCentralityComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<CentralityStatsResult> betweenessCentralityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, BetweennessCentralityStatsConfig::of);

        var computationResult = statsBusinessFacade.betweennessCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<CentralityMutateResult> betweenessCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, BetweennessCentralityMutateConfig::of);

        var computationResult = mutateBusinessFacade.betweennessCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toMutateResult(computationResult));
    }

    public Stream<CentralityWriteResult> betweenessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, BetweennessCentralityWriteConfig::of);

        var computationResult = writeBusinessFacade.betweennessCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toWriteResult(computationResult));
    }



    public Stream<MemoryEstimateResult> betweenessCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, BetweennessCentralityStreamConfig::of);

        return Stream.of(estimateBusinessFacade.betweennessCentrality(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> betweenessCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, BetweennessCentralityStatsConfig::of);

        return Stream.of(estimateBusinessFacade.betweennessCentrality(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> betweenessCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, BetweennessCentralityMutateConfig::of);

        return Stream.of(estimateBusinessFacade.betweennessCentrality(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> betweenessCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, BetweennessCentralityWriteConfig::of);

        return Stream.of(estimateBusinessFacade.betweennessCentrality(graphNameOrConfiguration, config));

    }

    public Stream<CentralityStreamResult> degreeCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createStreamConfig(configuration, DegreeCentralityStreamConfig::of);

        var computationResult = streamBusinessFacade.degreeCentrality(
            graphName,
            config
        );

        return DefaultCentralityComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<CentralityStatsResult> degreeCentralityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, DegreeCentralityStatsConfig::of);

        var computationResult = statsBusinessFacade.degreeCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<CentralityMutateResult> degreeCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, DegreeCentralityMutateConfig::of);

        var computationResult = mutateBusinessFacade.degreeCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toMutateResult(computationResult));
    }


    public Stream<CentralityWriteResult> degreeCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, DegreeCentralityWriteConfig::of);

        var computationResult = writeBusinessFacade.degreeCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toWriteResult(computationResult));
    }


    public Stream<MemoryEstimateResult> degreeCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, DegreeCentralityStreamConfig::of);

        return Stream.of(estimateBusinessFacade.degreeCentrality(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> degreeCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, DegreeCentralityStatsConfig::of);

        return Stream.of(estimateBusinessFacade.degreeCentrality(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> degreeCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, DegreeCentralityMutateConfig::of);

        return Stream.of(estimateBusinessFacade.degreeCentrality(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> degreeCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, DegreeCentralityWriteConfig::of);

        return Stream.of(estimateBusinessFacade.degreeCentrality(graphNameOrConfiguration, config));

    }

    public Stream<CentralityStreamResult> closenessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createStreamConfig(configuration, ClosenessCentralityStreamConfig::of);

        var computationResult = streamBusinessFacade.closenessCentrality(
            graphName,
            config
        );

        return DefaultCentralityComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<CentralityStatsResult> closenessCentralityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, ClosenessCentralityStatsConfig::of);

        var computationResult = statsBusinessFacade.closenessCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<CentralityMutateResult> closenessCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, ClosenessCentralityMutateConfig::of);

        var computationResult = mutateBusinessFacade.closenessCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toMutateResult(computationResult));
    }


    public Stream<CentralityWriteResult> closenessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, ClosenessCentralityWriteConfig::of);

        var computationResult = writeBusinessFacade.closenessCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toWriteResult(computationResult));
    }

    public Stream<BetaClosenessCentralityMutateResult> betaClosenessCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, ClosenessCentralityMutateConfig::of);

        var computationResult = mutateBusinessFacade.closenessCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(BetaClosenessCentralityComputationalResultTransformer.toMutateResult(
            computationResult,
            config
        ));
    }


    public Stream<BetaClosenessCentralityWriteResult> betaClosenessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, ClosenessCentralityWriteConfig::of);

        var computationResult = writeBusinessFacade.closenessCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(BetaClosenessCentralityComputationalResultTransformer.toWriteResult(
            computationResult,
            config
        ));
    }

    public Stream<CentralityStreamResult> harmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createStreamConfig(configuration, HarmonicCentralityStreamConfig::of);

        var computationResult = streamBusinessFacade.harmonicCentrality(
            graphName,
            config
        );

        return DefaultCentralityComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<CentralityStatsResult> harmonicCentralityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, HarmonicCentralityStatsConfig::of);

        var computationResult = statsBusinessFacade.harmonicCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<CentralityMutateResult> harmonicCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, HarmonicCentralityMutateConfig::of);

        var computationResult = mutateBusinessFacade.harmonicCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toMutateResult(computationResult));
    }


    public Stream<CentralityWriteResult> harmonicCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, HarmonicCentralityWriteConfig::of);

        var computationResult = writeBusinessFacade.harmonicCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toWriteResult(computationResult));
    }

    public Stream<AlphaHarmonicStreamResult> alphaHarmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, HarmonicCentralityStreamConfig::of);

        var computationResult = streamBusinessFacade.harmonicCentrality(
            graphName,
            config
        );

        return AlphaHarmonicCentralityComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<AlphaHarmonicWriteResult> alphaHarmonicCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, DeprecatedTieredHarmonicCentralityWriteConfig::of);

        var computationResult = writeBusinessFacade.alphaHarmonicCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(AlphaHarmonicCentralityComputationalResultTransformer.toWriteResult(
            computationResult,
            config
        ));
    }

    public Stream<CELFStreamResult> celfStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createStreamConfig(configuration, InfluenceMaximizationStreamConfig::of);

        var computationResult = streamBusinessFacade.celf(
            graphName,
            config
        );

        return CELFComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<MemoryEstimateResult> celfStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, InfluenceMaximizationStreamConfig::of);

        return Stream.of(estimateBusinessFacade.celf(graphNameOrConfiguration, config));

    }

    public Stream<CentralityStreamResult> pageRankStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createStreamConfig(configuration, PageRankStreamConfig::of);

        var computationResult = streamBusinessFacade.pageRank(
            graphName,
            config
        );

        return DefaultCentralityComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<MemoryEstimateResult> pageRankStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankStreamConfig::of);

        return Stream.of(estimateBusinessFacade.pageRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankStatsResult> pageRankStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankStatsConfig::of);

        var computationResult = statsBusinessFacade.pageRank(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(PageRankComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> pageRankStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankStatsConfig::of);

        return Stream.of(estimateBusinessFacade.pageRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankMutateResult> pageRankMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankMutateConfig::of);

        var computationResult = mutateBusinessFacade.pageRank(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(PageRankComputationalResultTransformer.toMutateResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> pageRankMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankMutateConfig::of);

        return Stream.of(estimateBusinessFacade.pageRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankWriteResult> pageRankWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankWriteConfig::of);

        var computationResult = writeBusinessFacade.pageRank(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(PageRankComputationalResultTransformer.toWriteResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> pageRankWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankWriteConfig::of);

        return Stream.of(estimateBusinessFacade.pageRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankStatsResult> articleRankStats(String graphName, Map<String, Object> configuration) {
        var config = createConfig(configuration, PageRankStatsConfig::of);

        var computationResult = statsBusinessFacade.articleRank(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(PageRankComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> articleRankStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankStatsConfig::of);

        return Stream.of(estimateBusinessFacade.articleRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankMutateResult> articleRankMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankMutateConfig::of);

        var computationResult = mutateBusinessFacade.articleRank(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(PageRankComputationalResultTransformer.toMutateResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> articleRankMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankMutateConfig::of);

        return Stream.of(estimateBusinessFacade.articleRank(graphNameOrConfiguration, config));
    }

    public Stream<CentralityStreamResult> articleRankStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createStreamConfig(configuration, PageRankStreamConfig::of);

        var computationResult = streamBusinessFacade.articleRank(
            graphName,
            config
        );

        return DefaultCentralityComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<MemoryEstimateResult> articleRankStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankStreamConfig::of);

        return Stream.of(estimateBusinessFacade.articleRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankWriteResult> articleRankWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankWriteConfig::of);

        var computationResult = writeBusinessFacade.articleRank(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(PageRankComputationalResultTransformer.toWriteResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> articleRankWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, PageRankWriteConfig::of);

        return Stream.of(estimateBusinessFacade.articleRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankMutateResult> eigenvectorMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = createConfig(configuration, PageRankMutateConfig::of);

        var computationResult = mutateBusinessFacade.eigenvector(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(PageRankComputationalResultTransformer.toMutateResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> eigenvectorMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = createConfig(configuration, PageRankMutateConfig::of);

        return Stream.of(estimateBusinessFacade.eigenvector(graphNameOrConfiguration, config));
    }

    public Stream<PageRankStatsResult> eigenvectorStats(String graphName, Map<String, Object> configuration) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = createConfig(configuration, PageRankStatsConfig::of);

        var computationResult = statsBusinessFacade.eigenvector(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(PageRankComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> eigenvectorStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = createConfig(configuration, PageRankStatsConfig::of);

        return Stream.of(estimateBusinessFacade.eigenvector(graphNameOrConfiguration, config));
    }

    public Stream<CentralityStreamResult> eigenvectorStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = createStreamConfig(configuration, PageRankStreamConfig::of);

        var computationResult = streamBusinessFacade.eigenvector(
            graphName,
            config
        );

        return DefaultCentralityComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<MemoryEstimateResult> eigenvectorStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = createConfig(configuration, PageRankStreamConfig::of);

        return Stream.of(estimateBusinessFacade.eigenvector(graphNameOrConfiguration, config));
    }

    public Stream<PageRankWriteResult> eigenvectorWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = createConfig(configuration, PageRankWriteConfig::of);

        var computationResult = writeBusinessFacade.eigenvector(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(PageRankComputationalResultTransformer.toWriteResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> eigenvectorWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = createConfig(configuration, PageRankWriteConfig::of);

        return Stream.of(estimateBusinessFacade.eigenvector(graphNameOrConfiguration, config));
    }

    // FIXME: this is abominable, we have to create separate configuration for Eigenvector that doesn't contain this key
    private static void eigenvectorConfigurationPreconditions(Map<String, Object> configuration) {
        if (configuration.containsKey("dampingFactor")) {
            throw new IllegalArgumentException("Unexpected configuration key: dampingFactor");
        }
    }


    // ################################################################################################################

    // FIXME: the following two methods are duplicate, find a good place for them.
    private <C extends AlgoBaseConfig> C createStreamConfig(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configCreator
    ) {
        return createConfig(
            configuration,
            configCreator.andThen(algorithmConfiguration -> {
                algorithmMetaDataSetter.set(algorithmConfiguration);
                return algorithmConfiguration;
            })
        );
    }

    private <C extends AlgoBaseConfig> C createConfig(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configCreator
    ) {
        return configurationParser.produceConfig(configuration, configCreator, user.getUsername());
    }
    //FIXME: here ends the fixme-block
}
