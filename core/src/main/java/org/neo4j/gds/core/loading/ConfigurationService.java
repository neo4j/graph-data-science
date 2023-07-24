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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is all about syntax checking/ dull boring parsing of disparate data into a nested data structure.
 * The behaviour selection in @GraphProjectFromStoreConfig#graphStoreFactory sticks out as belonging somewhere else.
 */
public class ConfigurationService {
    private static final Set<String> DISALLOWED_NATIVE_PROJECT_CONFIG_KEYS = Set.of(
        GraphProjectFromStoreConfig.NODE_PROJECTION_KEY,
        GraphProjectFromStoreConfig.RELATIONSHIP_PROJECTION_KEY
    );

    private static final Set<String> DISALLOWED_CYPHER_PROJECT_CONFIG_KEYS = Set.of(
        GraphProjectFromCypherConfig.NODE_QUERY_KEY,
        GraphProjectFromCypherConfig.RELATIONSHIP_QUERY_KEY
    );

    public GraphProjectFromStoreConfig parseNativeProjectConfiguration(
        User user,
        GraphName graphName,
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    ) {
        var wrappedRawConfiguration = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphProjectFromStoreConfig.of(
            user.getUsername(),
            graphName.getValue(),
            nodeProjection,
            relationshipProjection,
            wrappedRawConfiguration
        );

        validateProjectConfiguration(wrappedRawConfiguration, configuration, DISALLOWED_NATIVE_PROJECT_CONFIG_KEYS);

        return configuration;
    }

    public GraphProjectFromCypherConfig parseCypherProjectConfiguration(
        User user,
        GraphName graphName,
        String nodeQuery,
        String relationshipQuery,
        Map<String, Object> rawConfiguration
    ) {
        var wrappedRawConfiguration = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphProjectFromCypherConfig.of(
            user.getUsername(),
            graphName.getValue(),
            nodeQuery,
            relationshipQuery,
            wrappedRawConfiguration
        );

        validateProjectConfiguration(wrappedRawConfiguration, configuration, DISALLOWED_CYPHER_PROJECT_CONFIG_KEYS);

        return configuration;
    }

    public GraphProjectFromStoreConfig parseEstimateNativeProjectConfiguration(
        Object nodeProjection,
        Object relationshipProjection,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        /*
         * We should mint a type for estimation, instead of relying on this superset of stuff (the unused fields)
         */
        var configuration = GraphProjectFromStoreConfig.of(
            "unused",
            "unused",
            nodeProjection,
            relationshipProjection,
            cypherConfig
        );

        validateProjectConfiguration(cypherConfig, configuration, DISALLOWED_NATIVE_PROJECT_CONFIG_KEYS);

        return configuration;
    }

    private void validateProjectConfiguration(
        CypherMapAccess cypherConfig,
        GraphProjectConfig graphProjectConfig,
        Collection<String> disallowedConfigurationKeys
    ) {
        var allowedKeys = graphProjectConfig.isFictitiousLoading()
            ? graphProjectConfig.configKeys()
            : graphProjectConfig.configKeys()
                .stream()
                .filter(key -> !disallowedConfigurationKeys.contains(key))
                .collect(Collectors.toList());

        ensureOnlyAllowedKeysUsed(cypherConfig, allowedKeys);
    }

    private void ensureOnlyAllowedKeysUsed(CypherMapAccess cypherConfig, Collection<String> allowedKeys) {
        cypherConfig.requireOnlyKeysFrom(allowedKeys);
    }
}
