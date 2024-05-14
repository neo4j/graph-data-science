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
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.degree.DegreeCentralityStatsConfig;
import org.neo4j.gds.degree.DegreeCentralityStreamConfig;
import org.neo4j.gds.degree.DegreeCentralityWriteConfig;
import org.neo4j.gds.harmonic.DeprecatedTieredHarmonicCentralityWriteConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityMutateConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStatsConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityWriteConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationMutateConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStatsConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStreamConfig;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationWriteConfig;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.pagerank.PageRankStatsConfig;
import org.neo4j.gds.pagerank.PageRankStreamConfig;
import org.neo4j.gds.pagerank.PageRankWriteConfig;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityMutateResult;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityStatsResult;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityStreamResult;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityWriteResult;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.centrality.alphaharmonic.AlphaHarmonicStreamResult;
import org.neo4j.gds.procedures.centrality.alphaharmonic.AlphaHarmonicWriteResult;
import org.neo4j.gds.procedures.centrality.celf.CELFMutateResult;
import org.neo4j.gds.procedures.centrality.celf.CELFStatsResult;
import org.neo4j.gds.procedures.centrality.celf.CELFStreamResult;
import org.neo4j.gds.procedures.centrality.celf.CELFWriteResult;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankComputationalResultTransformer;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankMutateResult;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankStatsResult;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankWriteResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;

import java.util.Map;
import java.util.stream.Stream;

public class CentralityProcedureFacade {

    private final ConfigurationCreator configurationCreator;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final CentralityAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final CentralityAlgorithmsStatsBusinessFacade statsBusinessFacade;
    private final CentralityAlgorithmsStreamBusinessFacade streamBusinessFacade;
    private final CentralityAlgorithmsWriteBusinessFacade writeBusinessFacade;

    private final CentralityAlgorithmsEstimateBusinessFacade estimateBusinessFacade;

    public CentralityProcedureFacade(
        ConfigurationCreator configurationCreator,
        ProcedureReturnColumns procedureReturnColumns,
        CentralityAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        CentralityAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        CentralityAlgorithmsStatsBusinessFacade statsBusinessFacade,
        CentralityAlgorithmsStreamBusinessFacade streamBusinessFacade,
        CentralityAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.procedureReturnColumns = procedureReturnColumns;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.statsBusinessFacade = statsBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
        this.writeBusinessFacade = writeBusinessFacade;
        this.estimateBusinessFacade = estimateBusinessFacade;
    }

    public Stream<CentralityStreamResult> degreeCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfigurationForStream(configuration, DegreeCentralityStreamConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, DegreeCentralityStatsConfig::of);

        var computationResult = statsBusinessFacade.degreeCentrality(
            graphName,
            config,
            procedureReturnColumns.contains("centralityDistribution")
        );

        return Stream.of(DefaultCentralityComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<CentralityWriteResult> degreeCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, DegreeCentralityWriteConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, DegreeCentralityStreamConfig::of);

        return Stream.of(estimateBusinessFacade.degreeCentrality(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> degreeCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, DegreeCentralityStatsConfig::of);

        return Stream.of(estimateBusinessFacade.degreeCentrality(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> degreeCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, DegreeCentralityWriteConfig::of);

        return Stream.of(estimateBusinessFacade.degreeCentrality(graphNameOrConfiguration, config));

    }

    public Stream<CentralityStreamResult> harmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfigurationForStream(
            configuration,
            HarmonicCentralityStreamConfig::of
        );

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
        var config = configurationCreator.createConfiguration(configuration, HarmonicCentralityStatsConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, HarmonicCentralityMutateConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, HarmonicCentralityWriteConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, HarmonicCentralityStreamConfig::of);

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
        var config = configurationCreator.createConfiguration(
            configuration,
            DeprecatedTieredHarmonicCentralityWriteConfig::of
        );

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
        var config = configurationCreator.createConfigurationForStream(
            configuration,
            InfluenceMaximizationStreamConfig::of
        );

        var computationResult = streamBusinessFacade.celf(
            graphName,
            config
        );

        return CELFComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<CELFStatsResult> celfStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, InfluenceMaximizationStatsConfig::of);

        var statsResult = statsBusinessFacade.celf(
            graphName,
            config
        );

        return Stream.of(CELFComputationalResultTransformer.toStatsResult(statsResult, config));
    }

    public Stream<CELFMutateResult> celfMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, InfluenceMaximizationMutateConfig::of);

        var mutateResult = mutateBusinessFacade.celf(
            graphName,
            config
        );

        return Stream.of(CELFComputationalResultTransformer.toMutateResult(mutateResult));
    }

    public Stream<CELFWriteResult> celfWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, InfluenceMaximizationWriteConfig::of);

        var writeResult = writeBusinessFacade.celf(
            graphName,
            config
        );

        return Stream.of(CELFComputationalResultTransformer.toWriteResult(writeResult));
    }


    public Stream<MemoryEstimateResult> celfStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, InfluenceMaximizationStreamConfig::of);

        return Stream.of(estimateBusinessFacade.celf(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> celfStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, InfluenceMaximizationStatsConfig::of);

        return Stream.of(estimateBusinessFacade.celf(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> celfMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, InfluenceMaximizationMutateConfig::of);

        return Stream.of(estimateBusinessFacade.celf(graphNameOrConfiguration, config));

    }

    public Stream<MemoryEstimateResult> celfWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, InfluenceMaximizationWriteConfig::of);

        return Stream.of(estimateBusinessFacade.celf(graphNameOrConfiguration, config));

    }

    public Stream<CentralityStreamResult> pageRankStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfigurationForStream(configuration, PageRankStreamConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, PageRankStreamConfig::of);

        return Stream.of(estimateBusinessFacade.pageRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankStatsResult> pageRankStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, PageRankStatsConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, PageRankStatsConfig::of);

        return Stream.of(estimateBusinessFacade.pageRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankMutateResult> pageRankMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, PageRankMutateConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, PageRankMutateConfig::of);

        return Stream.of(estimateBusinessFacade.pageRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankWriteResult> pageRankWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, PageRankWriteConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, PageRankWriteConfig::of);

        return Stream.of(estimateBusinessFacade.pageRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankStatsResult> articleRankStats(String graphName, Map<String, Object> configuration) {
        var config = configurationCreator.createConfiguration(configuration, PageRankStatsConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, PageRankStatsConfig::of);

        return Stream.of(estimateBusinessFacade.articleRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankMutateResult> articleRankMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, PageRankMutateConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, PageRankMutateConfig::of);

        return Stream.of(estimateBusinessFacade.articleRank(graphNameOrConfiguration, config));
    }

    public Stream<CentralityStreamResult> articleRankStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfigurationForStream(configuration, PageRankStreamConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, PageRankStreamConfig::of);

        return Stream.of(estimateBusinessFacade.articleRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankWriteResult> articleRankWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, PageRankWriteConfig::of);

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
        var config = configurationCreator.createConfiguration(configuration, PageRankWriteConfig::of);

        return Stream.of(estimateBusinessFacade.articleRank(graphNameOrConfiguration, config));
    }

    public Stream<PageRankMutateResult> eigenvectorMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = configurationCreator.createConfiguration(configuration, PageRankMutateConfig::of);

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

        var config = configurationCreator.createConfiguration(configuration, PageRankMutateConfig::of);

        return Stream.of(estimateBusinessFacade.eigenvector(graphNameOrConfiguration, config));
    }

    public Stream<PageRankStatsResult> eigenvectorStats(String graphName, Map<String, Object> configuration) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = configurationCreator.createConfiguration(configuration, PageRankStatsConfig::of);

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

        var config = configurationCreator.createConfiguration(configuration, PageRankStatsConfig::of);

        return Stream.of(estimateBusinessFacade.eigenvector(graphNameOrConfiguration, config));
    }

    public Stream<CentralityStreamResult> eigenvectorStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = configurationCreator.createConfigurationForStream(configuration, PageRankStreamConfig::of);

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

        var config = configurationCreator.createConfiguration(configuration, PageRankStreamConfig::of);

        return Stream.of(estimateBusinessFacade.eigenvector(graphNameOrConfiguration, config));
    }

    public Stream<PageRankWriteResult> eigenvectorWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        eigenvectorConfigurationPreconditions(configuration);

        var config = configurationCreator.createConfiguration(configuration, PageRankWriteConfig::of);

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

        var config = configurationCreator.createConfiguration(configuration, PageRankWriteConfig::of);

        return Stream.of(estimateBusinessFacade.eigenvector(graphNameOrConfiguration, config));
    }

    // FIXME: this is abominable, we have to create separate configuration for Eigenvector that doesn't contain this key
    private static void eigenvectorConfigurationPreconditions(Map<String, Object> configuration) {
        if (configuration.containsKey("dampingFactor")) {
            throw new IllegalArgumentException("Unexpected configuration key: dampingFactor");
        }
    }


}
