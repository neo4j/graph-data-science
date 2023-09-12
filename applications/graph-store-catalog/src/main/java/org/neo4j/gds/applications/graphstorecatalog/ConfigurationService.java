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
package org.neo4j.gds.applications.graphstorecatalog;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.GraphDropNodePropertiesConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.config.GraphProjectFromGraphConfig;
import org.neo4j.gds.config.GraphRemoveGraphPropertiesConfig;
import org.neo4j.gds.config.GraphStreamGraphPropertiesConfig;
import org.neo4j.gds.config.GraphStreamRelationshipPropertiesConfig;
import org.neo4j.gds.config.GraphStreamRelationshipsConfig;
import org.neo4j.gds.config.WriteRelationshipPropertiesConfig;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.graphsampling.config.CommonNeighbourAwareRandomWalkConfig;
import org.neo4j.gds.projection.GraphProjectFromStoreConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public GraphProjectFromCypherConfig parseEstimateCypherProjectConfiguration(
        String nodeProjection,
        String relationshipProjection,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        /*
         * We should mint a type for estimation, instead of relying on this superset of stuff (the unused fields)
         */
        var configuration = GraphProjectFromCypherConfig.of(
            "unused",
            "unused",
            nodeProjection,
            relationshipProjection,
            cypherConfig
        );

        validateProjectConfiguration(cypherConfig, configuration, DISALLOWED_CYPHER_PROJECT_CONFIG_KEYS);

        return configuration;
    }

    public GraphProjectFromGraphConfig parseSubGraphProjectConfiguration(
        User user,
        GraphName graphName,
        GraphName originGraphName,
        String nodeFilter,
        String relationshipFilter,
        GraphStoreWithConfig originGraphConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphProjectFromGraphConfig.of(
            user.getUsername(),
            graphName.getValue(),
            originGraphName.getValue(),
            nodeFilter,
            relationshipFilter,
            originGraphConfiguration.config(),
            cypherConfig
        );

        validateProjectConfiguration(cypherConfig, configuration, Collections.emptySet());

        return configuration;
    }

    public GraphDropNodePropertiesConfig parseGraphDropNodePropertiesConfiguration(
        GraphName graphName,
        Object nodeProperties,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphDropNodePropertiesConfig.of(
            graphName.getValue(),
            nodeProperties,
            cypherConfig
        );

        ensureThereAreNoExtraConfigurationKeys(cypherConfig, configuration);

        return configuration;
    }

    public void validateDropGraphPropertiesConfiguration(
        GraphName graphName,
        String graphProperty,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphRemoveGraphPropertiesConfig.of(
            graphName.getValue(),
            graphProperty,
            cypherConfig
        );

        ensureThereAreNoExtraConfigurationKeys(cypherConfig, configuration);
    }

    public void validateGraphStreamGraphPropertiesConfig(
        GraphName graphName,
        String graphProperty,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphStreamGraphPropertiesConfig.of(
            graphName.getValue(),
            graphProperty,
            cypherConfig
        );

        ensureThereAreNoExtraConfigurationKeys(cypherConfig, configuration);
    }

    GraphStreamNodePropertiesConfig parseGraphStreamNodePropertiesConfiguration(
        GraphName graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphStreamNodePropertiesConfig.of(
            graphName.getValue(),
            nodeProperties,
            nodeLabels,
            cypherConfig
        );

        ensureThereAreNoExtraConfigurationKeys(cypherConfig, configuration);

        return configuration;
    }

    GraphStreamRelationshipPropertiesConfig parseGraphStreamRelationshipPropertiesConfiguration(
        GraphName graphName,
        List<String> relationshipProperties,
        List<String> relationshipTypes,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphStreamRelationshipPropertiesConfig.of(
            graphName.getValue(),
            relationshipProperties,
            relationshipTypes,
            cypherConfig
        );

        ensureThereAreNoExtraConfigurationKeys(cypherConfig, configuration);

        return configuration;
    }

    GraphStreamRelationshipsConfig parseGraphStreamRelationshipsConfiguration(
        GraphName graphName,
        List<String> relationshipTypes,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphStreamRelationshipsConfig.of(
            graphName.getValue(),
            relationshipTypes,
            cypherConfig
        );

        ensureThereAreNoExtraConfigurationKeys(cypherConfig, configuration);

        return configuration;
    }

    GraphWriteNodePropertiesConfig parseGraphWriteNodePropertiesConfiguration(
        GraphName graphName,
        Object nodeProperties,
        Object nodeLabels,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphWriteNodePropertiesConfig.of(
            graphName.getValue(),
            nodeProperties,
            nodeLabels,
            cypherConfig
        );

        ensureThereAreNoExtraConfigurationKeys(cypherConfig, configuration);

        return configuration;
    }

    WriteRelationshipPropertiesConfig parseWriteRelationshipPropertiesConfiguration(Map<String, Object> rawConfiguration) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        // no extra validation, these are just non-functional flags
        return WriteRelationshipPropertiesConfig.of(cypherConfig);
    }

    GraphWriteRelationshipConfig parseGraphWriteRelationshipConfiguration(
        String relationshipType,
        String relationshipProperty,
        Map<String, Object> rawConfiguration
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = GraphWriteRelationshipConfig.of(
            relationshipType,
            Optional.ofNullable(StringUtils.trimToNull(relationshipProperty)),
            cypherConfig
        );

        ensureThereAreNoExtraConfigurationKeys(cypherConfig, configuration);

        return configuration;
    }

    CommonNeighbourAwareRandomWalkConfig parseCommonNeighbourAwareRandomWalkConfig(Map<String, Object> rawConfiguration) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        return CommonNeighbourAwareRandomWalkConfig.of(cypherConfig);
    }

    private void ensureThereAreNoExtraConfigurationKeys(CypherMapAccess cypherConfig, BaseConfig config) {
        cypherConfig.requireOnlyKeysFrom(config.configKeys());
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
