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
package org.neo4j.gds.procedures.algorithms;

import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.procedures.configparser.ConfigurationParser;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ConfigurationCreator {
    private final ConfigurationParser configurationParser;
    private final AlgorithmMetaDataSetter algorithmMetaDataSetter;
    private final User user;

    public ConfigurationCreator(
        ConfigurationParser configurationParser,
        AlgorithmMetaDataSetter algorithmMetaDataSetter,
        User user
    ) {
        this.configurationParser = configurationParser;
        this.algorithmMetaDataSetter = algorithmMetaDataSetter;
        this.user = user;
    }


    public <C extends AlgoBaseConfig> C createConfiguration(
        Map<String, Object> rawConfiguration,
        BiFunction<String, CypherMapWrapper, C> parser
    ) {
        return configurationParser.produceConfig(rawConfiguration, parser, user);
    }

    public <C extends AlgoBaseConfig> C createConfigurationForStream(
        Map<String, Object> rawConfiguration,
        BiFunction<String, CypherMapWrapper, C> parser
    ) {
        C configuration = createConfiguration(rawConfiguration, parser);

        // yay, side effects
        algorithmMetaDataSetter.set(configuration);

        return configuration;
    }


    public <C extends AlgoBaseConfig> C createConfiguration(
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, C> parser
    ) {
        return createConfiguration(rawConfiguration, (__, cypherMapWrapper) -> parser.apply(cypherMapWrapper));
    }

    public <C extends AlgoBaseConfig> C createConfigurationForStream(
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, C> parser
    ) {
        return createConfigurationForStream(rawConfiguration, (__, cypherMapWrapper) -> parser.apply(cypherMapWrapper));

    }
}
