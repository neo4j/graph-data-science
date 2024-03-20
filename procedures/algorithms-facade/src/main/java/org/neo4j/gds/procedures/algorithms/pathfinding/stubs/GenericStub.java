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
package org.neo4j.gds.procedures.algorithms.pathfinding.stubs;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.ResultBuilder;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.AlgorithmHandle;
import org.neo4j.gds.results.MemoryEstimateResult;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class GenericStub {
    private final DefaultsConfiguration defaultsConfiguration;
    private final LimitsConfiguration limitsConfiguration;
    private final ConfigurationCreator configurationCreator;
    private final ConfigurationParser configurationParser;
    private final User user;
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;

    public GenericStub(
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        ConfigurationCreator configurationCreator,
        ConfigurationParser configurationParser,
        User user,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade
    ) {
        this.defaultsConfiguration = defaultsConfiguration;
        this.limitsConfiguration = limitsConfiguration;
        this.configurationCreator = configurationCreator;
        this.configurationParser = configurationParser;
        this.user = user;
        this.estimationFacade = estimationFacade;
    }

    /**
     * @see org.neo4j.gds.procedures.algorithms.pathfinding.MutateStub#validateConfiguration(java.util.Map)
     */
    <CONFIGURATION extends AlgoBaseConfig> void validateConfiguration(
        Function<CypherMapWrapper, CONFIGURATION> parser,
        Map<String, Object> configuration
    ) {
        configurationParser.parse(
            defaultsConfiguration,
            limitsConfiguration,
            Username.EMPTY_USERNAME.username(),
            configuration,
            (__, cmw) -> parser.apply(cmw)
        );
    }

    /**
     * @see org.neo4j.gds.procedures.algorithms.pathfinding.MutateStub#parseConfiguration(java.util.Map)
     */
    <CONFIGURATION extends AlgoBaseConfig> CONFIGURATION parseConfiguration(
        Function<CypherMapWrapper, CONFIGURATION> parser,
        Map<String, Object> configuration
    ) {
        return configurationParser.parse(
            defaultsConfiguration,
            limitsConfiguration,
            user.getUsername(),
            configuration,
            (__, cmw) -> parser.apply(cmw)
        );
    }

    /**
     * @see org.neo4j.gds.procedures.algorithms.pathfinding.MutateStub#getMemoryEstimation(String, java.util.Map)
     */
    <CONFIGURATION extends AlgoBaseConfig> MemoryEstimation getMemoryEstimation(
        String username,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> parser,
        Function<CONFIGURATION, MemoryEstimation> estimator
    ) {
        var configuration = configurationParser.parse(
            DefaultsConfiguration.Empty,
            LimitsConfiguration.Empty,
            username,
            rawConfiguration,
            (__, cmw) -> parser.apply(cmw)
        );

        return estimator.apply(configuration);
    }

    /**
     * @see org.neo4j.gds.procedures.algorithms.pathfinding.MutateStub#estimate(Object, java.util.Map)
     */
    <CONFIGURATION extends AlgoBaseConfig> Stream<MemoryEstimateResult> estimate(
        Object graphName,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> parser,
        Function<CONFIGURATION, MemoryEstimation> estimator
    ) {
        var memoryEstimation = getMemoryEstimation(user.getUsername(), rawConfiguration, parser, estimator);

        var configuration = parseConfiguration(parser, rawConfiguration);

        var memoryEstimateResult = estimationFacade.runEstimation(
            configuration,
            graphName,
            memoryEstimation
        );

        return Stream.of(memoryEstimateResult);
    }

    /**
     * @see org.neo4j.gds.procedures.algorithms.pathfinding.MutateStub#execute(String, java.util.Map)
     */
    <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> Stream<RESULT_TO_CALLER> execute(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> parser,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> executor,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfiguration(rawConfiguration, parser);

        var result = executor.compute(graphName, configuration, resultBuilder);

        return Stream.of(result);
    }
}
