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
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
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

        return BetweenessCentralityComputationalResultTransformer.toStreamResult(computationResult);
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

        return Stream.of(CentralityComputationalResultTransformer.toStatsResult(computationResult, config));
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

        return Stream.of(CentralityComputationalResultTransformer.toMutateResult(computationResult));
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

        return Stream.of(CentralityComputationalResultTransformer.toWriteResult(computationResult));
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
