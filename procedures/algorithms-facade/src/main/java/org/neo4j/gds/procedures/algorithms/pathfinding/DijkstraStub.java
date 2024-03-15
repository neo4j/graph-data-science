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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This is every single thing under the heading of Dijkstra mutate.
 */
public class DijkstraStub implements MutateStub<ShortestPathDijkstraMutateConfig, PathFindingMutateResult> {
    // global scope
    private final DefaultsConfiguration defaultsConfiguration;
    private final LimitsConfiguration limitsConfiguration;

    // request scoped dependencies
    private final ConfigurationCreator configurationCreator;
    private final User user;

    // structure
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithmsMutateModeBusinessFacade mutateFacade;
    private final ConfigurationParser configurationParser;

    DijkstraStub(
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        ConfigurationCreator configurationCreator,
        ConfigurationParser configurationParser,
        User user,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithmsMutateModeBusinessFacade mutateFacade
    ) {
        this.defaultsConfiguration = defaultsConfiguration;
        this.limitsConfiguration = limitsConfiguration;
        this.configurationCreator = configurationCreator;
        this.configurationParser = configurationParser;
        this.user = user;
        this.estimationFacade = estimationFacade;
        this.mutateFacade = mutateFacade;
    }

    @Override
    public void validateConfiguration(Map<String, Object> configuration) {
        var ignored = parseAndValidateConfiguration(
            Username.EMPTY_USERNAME.username(),
            configuration,
            defaultsConfiguration,
            limitsConfiguration
        );
    }

    @Override
    public ShortestPathDijkstraMutateConfig parseConfiguration(Map<String, Object> configuration) {
        return parseAndValidateConfiguration(
            user.getUsername(),
            configuration,
            defaultsConfiguration,
            limitsConfiguration
        );
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> rawConfiguration) {
        var configuration = parseAndValidateConfiguration(
            username,
            rawConfiguration,
            DefaultsConfiguration.Empty,
            LimitsConfiguration.Empty
        );

        return estimationFacade.singleSourceShortestPathDijkstraEstimation(configuration);
    }

    @Override
    public Stream<PathFindingMutateResult> execute(String graphName, Map<String, Object> configuration) {
        return Stream.of(
            runMutateAlgorithm(
                graphName,
                configuration,
                ShortestPathDijkstraMutateConfig::of,
                mutateFacade::singlePairShortestPathDijkstraMutate
            )
        );
    }

    /**
     * Fully configurable so we can capture all variants
     *
     * @deprecated Reuse between stubs
     */
    @Deprecated
    private ShortestPathDijkstraMutateConfig parseAndValidateConfiguration(
        String username,
        Map<String, Object> configuration,
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration
    ) {
        return configurationParser.parse(
            defaultsConfiguration,
            limitsConfiguration,
            username,
            configuration,
            (__, cypherMapWrapper) -> ShortestPathDijkstraMutateConfig.of(cypherMapWrapper)
        );
    }

    /**
     * @deprecated Reuse this between stubs
     */
    @Deprecated
    private <CONFIGURATION extends AlgoBaseConfig> PathFindingMutateResult runMutateAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, PathFindingResult, PathFindingMutateResult> algorithm
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfiguration(rawConfiguration, configurationSupplier);

        // mutation
        var resultBuilder = new PathFindingResultBuilderForMutateMode(configuration);

        return algorithm.compute(graphName, configuration, resultBuilder);
    }
}
