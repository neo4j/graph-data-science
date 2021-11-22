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
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.config.BaseConfig.SUDO_KEY;
import static org.neo4j.gds.config.ConcurrencyConfig.CONCURRENCY_KEY;
import static org.neo4j.gds.config.ConcurrencyConfig.DEFAULT_CONCURRENCY;
import static org.neo4j.gds.config.GraphCreateConfig.READ_CONCURRENCY_KEY;

public class DefaultProcConfigParser<CONFIG extends AlgoBaseConfig> implements ProcConfigParser<CONFIG> {

    private final NewConfigFunction<CONFIG> newConfigFunction;
    private final String username;
    private Map<String, Class<?>> sharedConfigKeys;

    interface NewConfigFunction<CONFIG extends AlgoBaseConfig> {
        CONFIG apply(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
        );
    }

    DefaultProcConfigParser(String username, NewConfigFunction<CONFIG> newConfigFunction) {
        this.username = username;
        this.sharedConfigKeys = new HashMap<>();
        this.newConfigFunction = newConfigFunction;
    }

    @Override
    public String username() {
        return this.username;
    }

    @Override
    public Pair<CONFIG, Optional<String>> processInput(Object graphNameOrConfig, Map<String, Object> configuration) {
        CONFIG config;
        Optional<String> graphName = Optional.empty();

        if (graphNameOrConfig instanceof String) {
            graphName = Optional.of((String) graphNameOrConfig);
            CypherMapWrapper algoConfig = CypherMapWrapper.create(configuration);
            config = newConfig(graphName, algoConfig);

            //TODO: assert that algoConfig is empty or fail
        } else if (graphNameOrConfig instanceof Map) {
            if (!configuration.isEmpty()) {
                throw new IllegalArgumentException(
                    "The second parameter can only used when a graph name is given as first parameter");
            }

            Map<String, Object> implicitConfig = (Map<String, Object>) graphNameOrConfig;
            CypherMapWrapper implicitAndAlgoConfig = CypherMapWrapper.create(implicitConfig);

            config = newConfig(Optional.empty(), implicitAndAlgoConfig);

            //TODO: assert that implicitAndAlgoConfig is empty or fail
        } else {
            throw new IllegalArgumentException(
                "The first parameter must be a graph name or a configuration map, but was: " + graphNameOrConfig
            );
        }

        return Tuples.pair(config, graphName);
    }

    @Override
    public CONFIG newConfig(Optional<String> graphName, CypherMapWrapper config) {
        Optional<GraphCreateConfig> maybeImplicitCreate = Optional.empty();
        Collection<String> allowedKeys = new HashSet<>();
        // implicit loading
        if (graphName.isEmpty()) {
            // inherit concurrency from AlgoBaseConfig
            if (!config.containsKey(READ_CONCURRENCY_KEY)) {
                config = config.withNumber(READ_CONCURRENCY_KEY, config.getInt(CONCURRENCY_KEY, DEFAULT_CONCURRENCY));
            }
            GraphCreateConfig createConfig = GraphCreateConfig.createImplicit(username, config);
            maybeImplicitCreate = Optional.of(createConfig);
            allowedKeys.addAll(createConfig.configKeys());
            CypherMapWrapper configWithoutCreateKeys = config.withoutAny(allowedKeys);
            // check if we have an explicit configured sudo key, as this one is
            // shared between create and algo configs
            for (var entry : allSharedConfigKeys().entrySet()) {
                var value = config.getChecked(entry.getKey(), null, entry.getValue());
                if (value != null) {
                    configWithoutCreateKeys = configWithoutCreateKeys.withEntry(entry.getKey(), value);
                }
            }
            config = configWithoutCreateKeys;
        }
        CONFIG algoConfig = newConfigFunction.apply(username, graphName, maybeImplicitCreate, config);
        allowedKeys.addAll(algoConfig.configKeys());
        validateConfig(config, allowedKeys);
        return algoConfig;
    }

    /**
     * If the algorithm config shares any configuration parameters with anonymous projections, these must be declared here.
     */
    @Override
    public void withSharedConfigKeys(Map<String, Class<?>> sharedConfigKeys) {
        this.sharedConfigKeys = sharedConfigKeys;
    }

    private void validateConfig(CypherMapWrapper cypherConfig, Collection<String> allowedKeys) {
        cypherConfig.requireOnlyKeysFrom(allowedKeys);
    }

    private Map<String, Class<?>> allSharedConfigKeys() {
        var configKeys = new HashMap<>(sharedConfigKeys);
        configKeys.put(SUDO_KEY, Boolean.class);
        return configKeys;
    }
}
