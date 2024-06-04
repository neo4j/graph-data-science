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
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.pagerank.PageRankStreamConfig;
import org.neo4j.gds.pagerank.PageRankWriteConfig;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityStreamResult;
import org.neo4j.gds.procedures.algorithms.centrality.PageRankWriteResult;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankComputationalResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

public class CentralityProcedureFacade {

    private final ConfigurationCreator configurationCreator;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final CentralityAlgorithmsStreamBusinessFacade streamBusinessFacade;
    private final CentralityAlgorithmsWriteBusinessFacade writeBusinessFacade;

    private final CentralityAlgorithmsEstimateBusinessFacade estimateBusinessFacade;

    public CentralityProcedureFacade(
        ConfigurationCreator configurationCreator,
        ProcedureReturnColumns procedureReturnColumns,
        CentralityAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        CentralityAlgorithmsStreamBusinessFacade streamBusinessFacade,
        CentralityAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.procedureReturnColumns = procedureReturnColumns;
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
