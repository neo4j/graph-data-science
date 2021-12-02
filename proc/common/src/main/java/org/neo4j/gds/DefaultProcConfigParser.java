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
package org.neo4j.gds;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class DefaultProcConfigParser<CONFIG extends AlgoBaseConfig> implements ProcConfigParser<CONFIG, Pair<CONFIG, Optional<String>>> {

    private final NewConfigFunction<CONFIG> newConfigFunction;
    private final String username;

    interface NewConfigFunction<CONFIG extends AlgoBaseConfig> {
        CONFIG apply(
            String username,
            CypherMapWrapper config
        );
    }

    DefaultProcConfigParser(String username, NewConfigFunction<CONFIG> newConfigFunction) {
        this.username = username;
        this.newConfigFunction = newConfigFunction;
    }

    @Override
    public String username() {
        return this.username;
    }

    @Override
    public Pair<CONFIG, Optional<String>> processInput(Object graphNameOrConfig, Map<String, Object> configuration) {
        if (graphNameOrConfig instanceof String) {
            var graphName = Optional.of((String) graphNameOrConfig);
            CypherMapWrapper algoConfig = CypherMapWrapper.create(configuration);
            var config = newConfig(graphName, algoConfig);
            return Tuples.pair(config, graphName);
        }
        throw new IllegalArgumentException(
            "The first parameter must be a graph name, but was: " + graphNameOrConfig
        );
    }

    @Override
    public CONFIG newConfig(Optional<String> graphName, CypherMapWrapper config) {
        CONFIG algoConfig = newConfigFunction.apply(username, config);
        validateConfig(config, algoConfig.configKeys());
        return algoConfig;
    }

    private void validateConfig(CypherMapWrapper cypherConfig, Collection<String> allowedKeys) {
        cypherConfig.requireOnlyKeysFrom(allowedKeys);
    }
}
