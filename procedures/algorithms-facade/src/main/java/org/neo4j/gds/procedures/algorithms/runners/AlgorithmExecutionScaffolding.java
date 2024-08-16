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

import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.procedures.algorithms.AlgorithmHandle;
import org.neo4j.gds.procedures.algorithms.StreamAlgorithmHandle;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class AlgorithmExecutionScaffolding {
    private final ConfigurationParser configurationParser;
    private final AlgorithmMetaDataSetter algorithmMetaDataSetter;
    private final User user;

    public AlgorithmExecutionScaffolding(
        ConfigurationParser configurationParser,
        AlgorithmMetaDataSetter algorithmMetaDataSetter,
        User user
    ) {
        this.configurationParser = configurationParser;
        this.algorithmMetaDataSetter = algorithmMetaDataSetter;
        this.user = user;
    }

    public <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> RESULT_TO_CALLER runAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationLexer,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> algorithm,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        var graphName = GraphName.parse(graphNameAsString);

        var configuration = configurationParser.parseConfiguration(
            rawConfiguration,
            configurationLexer,
            user
        );

        return algorithm.compute(graphName, configuration, resultBuilder);
    }


    public <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> Stream<RESULT_TO_CALLER> runStreamAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationLexer,
        StreamAlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> algorithm,
        StreamResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationParser.parseConfiguration(
            rawConfiguration,
            configurationLexer,
            user
        );
        StreamAlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> actual =
            (g, c, r) -> {
                // the shoehorning
                algorithmMetaDataSetter.set(c);
                return algorithm.compute(g, c, r);
            };

        return actual.compute(graphName, configuration, resultBuilder);
    }


}
