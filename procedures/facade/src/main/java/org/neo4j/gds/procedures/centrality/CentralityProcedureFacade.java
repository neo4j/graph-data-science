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
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.pagerank.PageRankStatsConfig;
import org.neo4j.gds.pagerank.PageRankStreamConfig;
import org.neo4j.gds.pagerank.PageRankWriteConfig;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityStreamResult;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankComputationalResultTransformer;
import org.neo4j.gds.procedures.algorithms.centrality.PageRankMutateResult;
import org.neo4j.gds.procedures.algorithms.centrality.PageRankStatsResult;
import org.neo4j.gds.procedures.algorithms.centrality.PageRankWriteResult;

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
}
