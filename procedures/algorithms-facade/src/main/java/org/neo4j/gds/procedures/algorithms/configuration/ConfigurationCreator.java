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
package org.neo4j.gds.procedures.algorithms.configuration;

import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;
import java.util.Optional;
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

    public <CONFIGURATION extends AlgoBaseConfig> CONFIGURATION createConfiguration(
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> parser,
        Optional<ConfigurationValidationHook<CONFIGURATION>> configurationValidation
    ) {
        return parseAndValidate(rawConfiguration, parser, configurationValidation);
    }

    public <CONFIGURATION extends AlgoBaseConfig> CONFIGURATION createConfigurationForStream(
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> parser,
        Optional<ConfigurationValidationHook<CONFIGURATION>> configurationValidation
    ) {
        CONFIGURATION configuration = parseAndValidate(rawConfiguration, parser, configurationValidation);

        // yay, side effects
        algorithmMetaDataSetter.set(configuration);

        return configuration;
    }

    private <CONFIGURATION extends AlgoBaseConfig> CONFIGURATION parseAndValidate(
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> parser,
        Optional<ConfigurationValidationHook<CONFIGURATION>> configurationValidation
    ) {
        CONFIGURATION configuration = configurationParser.parseConfiguration(
            rawConfiguration,
            (__, cypherMapWrapper) -> parser.apply(cypherMapWrapper),
            user
        );

        configurationValidation.ifPresent(hook -> hook.onConfigurationParsed(configuration));

        return configuration;
    }
}
