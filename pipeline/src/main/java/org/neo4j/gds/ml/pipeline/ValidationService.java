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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.procedures.algorithms.Algorithm;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;

import java.util.Map;
import java.util.function.Function;

class ValidationService {
    private final ConfigurationParsers configurationParsers = new ConfigurationParsers();

    private final DefaultsConfiguration defaultsConfiguration;
    private final LimitsConfiguration limitsConfiguration;
    private final ConfigurationParser configurationParser;

    ValidationService(
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        ConfigurationParser configurationParser
    ) {
        this.defaultsConfiguration = defaultsConfiguration;
        this.limitsConfiguration = limitsConfiguration;
        this.configurationParser = configurationParser;
    }

    /**
     * For pipelines, when adding steps, we need to validate them without considering the current user.
     * So we effectively get validation against global defaults and limits.
     *
     * @throws IllegalArgumentException if the procedure name did not match an algorithm
     */
    void validate(Algorithm algorithm, Map<String, Object> configuration) {
        var parser = configurationParsers.lookup(algorithm);

        validateAnonymously(parser, configuration);
    }

    /**
     * Anonymously meaning no user, so no per-user defaults and limits
     */
    private <CONFIGURATION extends AlgoBaseConfig> void validateAnonymously(
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
}
