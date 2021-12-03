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

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class AlgoConfigParser<CONFIG extends AlgoBaseConfig> implements ProcConfigParser<CONFIG> {

    private final NewConfigFunction<CONFIG> newConfigFunction;
    private final String username;

    interface NewConfigFunction<CONFIG extends AlgoBaseConfig> {
        CONFIG apply(
            String username,
            CypherMapWrapper config
        );
    }

    AlgoConfigParser(String username, NewConfigFunction<CONFIG> newConfigFunction) {
        this.username = username;
        this.newConfigFunction = newConfigFunction;
    }

    @Override
    public String username() {
        return this.username;
    }

    @Override
    public CONFIG processInput(Map<String, Object> configuration) {
        CypherMapWrapper algoConfig = CypherMapWrapper.create(configuration);
        return newConfig(Optional.empty(), algoConfig);
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
