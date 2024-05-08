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
package org.neo4j.gds.procedures.algorithms.runners;

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.procedures.algorithms.AlgorithmHandle;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class StreamModeAlgorithmRunner {
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final ConfigurationCreator configurationCreator;

    public StreamModeAlgorithmRunner(
        CloseableResourceRegistry closeableResourceRegistry,
        ConfigurationCreator configurationCreator
    ) {
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.configurationCreator = configurationCreator;
    }

    /**
     * Some reuse, all the stream algorithms use the same high level structure:
     * <ol>
     *     <li> configuration parsing
     *     <li> parameter marshalling
     *     <li> delegating to down stream layer to call the thing we are actually interested in
     *     <li> handle resource closure
     * </ol>
     */
    public <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> Stream<RESULT_TO_CALLER> runStreamModeAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>, Void> resultBuilder,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>, Void> algorithm
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfigurationForStream(rawConfiguration, configurationSupplier);

        var resultStream = algorithm.compute(graphName, configuration, resultBuilder);

        // we need to do this for stream mode
        closeableResourceRegistry.register(resultStream);

        return resultStream;
    }
}
