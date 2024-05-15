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
package org.neo4j.gds.projection;

import org.intellij.lang.annotations.PrintFormat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo.DatabaseLocation;
import org.neo4j.gds.api.ImmutableDatabaseInfo;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.compat.CompatUserAggregator;
import org.neo4j.gds.core.ConfigKeyValidation;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.LazyIdMapBuilder;
import org.neo4j.gds.core.loading.LazyIdMapBuilderBuilder;
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.PropertyValues;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.neo4j.gds.projection.GraphImporter.NO_TARGET_NODE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class GraphAggregator implements CompatUserAggregator {

    static final String SOURCE_NODE_PROPERTIES = "sourceNodeProperties";
    static final String SOURCE_NODE_LABELS = "sourceNodeLabels";
    static final String TARGET_NODE_PROPERTIES = "targetNodeProperties";
    static final String TARGET_NODE_LABELS = "targetNodeLabels";
    static final String ALPHA_RELATIONSHIP_PROPERTIES = "properties";
    static final String RELATIONSHIP_PROPERTIES = "relationshipProperties";
    private static final String RELATIONSHIP_TYPE = "relationshipType";

    private final DatabaseId databaseId;
    private final String username;
    private final WriteMode writeMode;
    private final ExecutingQueryProvider queryProvider;
    private final ProjectionMetricsService projectionMetricsService;

    private final ProgressTimer progressTimer;
    private final ConfigValidator configValidator;

    // Used for initializing the data and rel importers
    private final Lock lock;
    private final ExtractNodeId extractNodeId;
    private volatile @Nullable GraphImporter importer;

    // #result() may be called twice, we cache the result of the first call to return it again in the second invocation
    private @Nullable AggregationResult result;

    GraphAggregator(
        DatabaseId databaseId,
        String username,
        WriteMode writeMode,
        ExecutingQueryProvider queryProvider,
        ProjectionMetricsService projectionMetricsService
    ) {
        this.databaseId = databaseId;
        this.username = username;
        this.writeMode = writeMode;
        this.queryProvider = queryProvider;
        this.projectionMetricsService = projectionMetricsService;
        this.progressTimer = ProgressTimer.start();
        this.lock = new ReentrantLock();
        this.configValidator = new ConfigValidator();
        this.extractNodeId = new ExtractNodeId();
    }

    void projectNextRelationship(
        TextValue graphName,
        AnyValue sourceNode,
        AnyValue targetNode,
        AnyValue dataConfig,
        AnyValue config,
        AnyValue migrationConfig
    ) {
        this.configValidator.validateConfig(dataConfig, config, migrationConfig);

        var data = initGraphData(graphName, config);

        @Nullable PropertyValues sourceNodePropertyValues = null;
        @Nullable PropertyValues targetNodePropertyValues = null;
        NodeLabelToken sourceNodeLabels = NodeLabelTokens.missing();
        NodeLabelToken targetNodeLabels = NodeLabelTokens.missing();

        if (dataConfig instanceof MapValue) {
            sourceNodePropertyValues = propertiesConfig(SOURCE_NODE_PROPERTIES, (MapValue) dataConfig);
            sourceNodeLabels = labelsConfig(SOURCE_NODE_LABELS, (MapValue) dataConfig);

            if (targetNode != NoValue.NO_VALUE) {
                targetNodePropertyValues = propertiesConfig(TARGET_NODE_PROPERTIES, (MapValue) dataConfig);
                targetNodeLabels = labelsConfig(TARGET_NODE_LABELS, (MapValue) dataConfig);
            }
        }

        PropertyValues relationshipProperties = null;
        RelationshipType relationshipType = RelationshipType.ALL_RELATIONSHIPS;

        if (dataConfig instanceof MapValue) {
            relationshipProperties = propertiesConfig(RELATIONSHIP_PROPERTIES, (MapValue) dataConfig);
            relationshipType = typeConfig(RELATIONSHIP_TYPE, (MapValue) dataConfig);
        }

        data.update(
            extractNodeId(sourceNode),
            targetNode == NoValue.NO_VALUE ? NO_TARGET_NODE : extractNodeId(targetNode),
            sourceNodePropertyValues,
            targetNodePropertyValues,
            sourceNodeLabels,
            targetNodeLabels,
            relationshipType,
            relationshipProperties
        );
    }

    private GraphImporter initGraphData(TextValue graphName, AnyValue config) {
        var data = this.importer;
        if (data != null) {
            return data;
        }

        this.lock.lock();
        try {
            data = this.importer;
            if (data == null) {
                this.importer = data = createGraphImporter(graphName, config);
            }
            return data;
        } finally {
            this.lock.unlock();
        }
    }

    private GraphImporter createGraphImporter(
        TextValue graphNameValue,
        AnyValue configMap
    ) {
        var graphName = graphNameValue.stringValue();
        var query = this.queryProvider.executingQuery().orElse("");

        validateGraphName(graphName, this.username, this.databaseId);
        var config = GraphProjectFromCypherAggregationConfig.of(
            this.username,
            graphName,
            query,
            (configMap instanceof MapValue) ? (MapValue) configMap : MapValue.EMPTY
        );

        var idMapBuilder = idMapBuilder(config.readConcurrency());

        return new GraphImporter(
            config,
            config.undirectedRelationshipTypes(),
            config.inverseIndexedRelationshipTypes(),
            idMapBuilder,
            this.writeMode,
            query
        );
    }

    private static LazyIdMapBuilder idMapBuilder(Concurrency readConcurrency) {
        return new LazyIdMapBuilderBuilder()
            .concurrency(readConcurrency)
            .hasLabelInformation(true)
            .hasProperties(true)
            .propertyState(PropertyState.PERSISTENT)
            .build();
    }

    private static void validateGraphName(String graphName, String username, DatabaseId databaseId) {
        if (GraphStoreCatalog.exists(username, databaseId, graphName)) {
            throw new IllegalArgumentException("Graph " + graphName + " already exists");
        }
    }

    private static NodeLabelToken labelsConfig(String nodeLabelKey, @NotNull MapValue nodesConfig) {
        var nodeLabelsEntry = nodesConfig.get(nodeLabelKey);
        return tryLabelsConfig(nodeLabelsEntry, nodeLabelKey);
    }

    private static NodeLabelToken tryLabelsConfig(AnyValue nodeLabels, String nodeLabelKey) {
        var nodeLabelToken = nodeLabels.map(ReadNodeLabels.INSTANCE);

        if (nodeLabelToken.isInvalid()) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "The value of `%s` must be either a `List of Strings`, a `String`, or a `Boolean`, but was `%s`.",
                    nodeLabelKey,
                    nodeLabels.getTypeName()
                )
            );
        }

        return nodeLabelToken;
    }

    @Override
    public AnyValue result() throws ProcedureException {
        var projectionMetric = projectionMetricsService.createCypherV2();
        AggregationResult result;
        try(projectionMetric) {
            projectionMetric.start();
            result = buildGraph();
        } catch (Exception e) {
            projectionMetric.failed(e);
            throw new ProcedureException(
                Status.Procedure.ProcedureCallFailed,
                e,
                "Failed to invoke function `%s`: Caused by: %s",
                CypherAggregation.FUNCTION_NAME,
                e
            );
        }

        if (result == null) {
            return Values.NO_VALUE;
        }

        var builder = new MapValueBuilder(6);
        builder.add("graphName", Values.stringValue(result.graphName()));
        builder.add("nodeCount", Values.longValue(result.nodeCount()));
        builder.add("relationshipCount", Values.longValue(result.relationshipCount()));
        builder.add("projectMillis", Values.longValue(result.projectMillis()));
        builder.add("configuration", ValueUtils.asAnyValue(result.configuration()));
        builder.add("query", ValueUtils.asAnyValue(result.query()));
        return builder.build();
    }

    public @Nullable AggregationResult buildGraph() {
        var importer = this.importer;
        if (importer == null) {
            // Nothing aggregated
            return null;
        }

        // Older cypher runtimes call the result method multiple times, we cache the result of the first call
        if (this.result != null) {
            return this.result;
        }

        var databaseInfo = ImmutableDatabaseInfo.builder()
            .databaseId(this.databaseId)
            .databaseLocation(DatabaseLocation.LOCAL)
            .build();

        this.result = importer.result(
            databaseInfo,
            this.progressTimer,
            extractNodeId.hasSeenArbitraryIds()
        );

        return this.result;
    }

    private long extractNodeId(@NotNull AnyValue node) {
        return node.map(this.extractNodeId);
    }

    @Nullable
    static PropertyValues propertiesConfig(String key, @NotNull MapValue container) {
        var properties = container.get(key);
        if (properties instanceof MapValue) {
            var mapProperties = (MapValue) properties;
            if (mapProperties.isEmpty()) {
                return null;
            }
            return PropertyValues.of(mapProperties);
        }

        if (properties == NoValue.NO_VALUE) {
            return null;
        }

        throw new IllegalArgumentException(
            formatWithLocale(
                "The value of `%s` must be a `Map of Property Values`, but was `%s`.",
                key,
                properties.getTypeName()
            )
        );
    }

    private static RelationshipType typeConfig(
        @SuppressWarnings("SameParameterValue") String relationshipTypeKey,
        @NotNull MapValue relationshipConfig
    ) {
        var relationshipTypeEntry = relationshipConfig.get(relationshipTypeKey);
        if (relationshipTypeEntry instanceof TextValue) {
            return RelationshipType.of(((TextValue) relationshipTypeEntry).stringValue());
        }
        if (relationshipTypeEntry == NoValue.NO_VALUE) {
            return RelationshipType.ALL_RELATIONSHIPS;
        }

        throw new IllegalArgumentException(
            formatWithLocale(
                "The value of `%s` must be `String`, but was `%s`.",
                relationshipTypeKey,
                relationshipTypeEntry.valueRepresentation().valueGroup()
            )
        );
    }

    public static final class ConfigValidator {
        private static final Set<String> DATA_CONFIG_KEYS = Set.of(
            SOURCE_NODE_PROPERTIES,
            SOURCE_NODE_LABELS,
            TARGET_NODE_PROPERTIES,
            TARGET_NODE_LABELS,
            RELATIONSHIP_PROPERTIES,
            RELATIONSHIP_TYPE
        );

        private static final Set<String> PROJECTION_CONFIG_KEYS = Set.copyOf(
            GraphProjectFromCypherAggregationConfig.of("", "", "", MapValue.EMPTY).configKeys()
        );

        private final AtomicBoolean validate = new AtomicBoolean(true);

        public void validateConfig(AnyValue dataConfig, AnyValue projectionConfig, AnyValue migrationConfig) {
            if (dataConfig instanceof MapValue || projectionConfig instanceof MapValue) {
                if (this.validate.get()) {
                    if (this.validate.getAndSet(false)) {
                        if (dataConfig instanceof MapValue) {
                            validateDataConfig((MapValue) dataConfig, projectionConfig);
                        }
                        if (projectionConfig instanceof MapValue) {
                            validateProjectionConfig((MapValue) projectionConfig, migrationConfig);
                        }
                    }
                }
            }
        }

        private void validateDataConfig(MapValue dataConfig, AnyValue projectionConfig) {
            checkForNotMigratedConfigKeys(dataConfig);

            // most map implementation create a new collection, unlike what most Java collections might do, so we cache it.
            var dataConfigKeys = mapKeys(dataConfig);

            checkForMergedOrSwappedOrForgottenConfig(projectionConfig, dataConfigKeys);

            ConfigKeyValidation.requireOnlyKeysFrom(DATA_CONFIG_KEYS, dataConfigKeys);

            checkForMutuallyRequiredKeys(dataConfig, SOURCE_NODE_LABELS, TARGET_NODE_LABELS);
            checkForMutuallyRequiredKeys(dataConfig, TARGET_NODE_LABELS, SOURCE_NODE_LABELS);
            checkForMutuallyRequiredKeys(dataConfig, SOURCE_NODE_PROPERTIES, TARGET_NODE_PROPERTIES);
            checkForMutuallyRequiredKeys(dataConfig, TARGET_NODE_PROPERTIES, SOURCE_NODE_PROPERTIES);
        }

        private void validateProjectionConfig(MapValue projectionConfig, AnyValue migrationConfig) {
            var containsRelationshipKeys = projectionConfig.containsKey(RELATIONSHIP_PROPERTIES) || projectionConfig
                .containsKey(RELATIONSHIP_TYPE) || projectionConfig.containsKey(ALPHA_RELATIONSHIP_PROPERTIES);

            var configAsAlphaParameter = migrationConfig != NoValue.NO_VALUE;

            if (containsRelationshipKeys || configAsAlphaParameter) {
                throw error(
                    "The parameters for `nodesConfig` and `relationshipsConfig` have been merged. " +
                        "Update your query by merging the 4th and 5th parameter into one parameter."
                );
            }
        }

        private static void checkForNotMigratedConfigKeys(MapValue dataConfig) {
            if (dataConfig.containsKey(ALPHA_RELATIONSHIP_PROPERTIES)) {
                throw error(
                    "The configuration key '%s' is now called '%s'.",
                    ALPHA_RELATIONSHIP_PROPERTIES,
                    RELATIONSHIP_PROPERTIES
                );
            }
        }

        private static void checkForMergedOrSwappedOrForgottenConfig(
            AnyValue projectionConfig,
            Collection<String> dataConfigKeys
        ) {
            if (dataConfigKeys.stream().anyMatch(PROJECTION_CONFIG_KEYS::contains)) {
                checkForSwappedOrForgottenConfig(projectionConfig, dataConfigKeys);
                checkForMergedConfig(dataConfigKeys);
            }
        }

        private static void checkForSwappedOrForgottenConfig(
            AnyValue projectionConfig,
            Collection<String> dataConfigKeys
        ) {
            if (PROJECTION_CONFIG_KEYS.containsAll(dataConfigKeys)) {
                if (projectionConfig == NoValue.NO_VALUE) {
                    throw error(
                        "The `dataConfig` configuration parameter is missing. " +
                            "If you meant to provide an empty configuration for the 4th parameter, " +
                            "you can pass an empty map: '{}'."
                    );
                } else {
                    throw error(
                        "The configuration parameters are provided in the wrong order. " +
                            "Update your query by swapping the 4th and 5th parameter."
                    );
                }
            }
        }

        private static void checkForMergedConfig(Collection<String> dataConfigKeys) {
            if (dataConfigKeys.stream().anyMatch(DATA_CONFIG_KEYS::contains)) {
                throw error(
                    "The configuration parameters are merged and provided as one parameter. " +
                        "Update your query by splitting the configuration into two parameters. " +
                        "Refer to the documentation for details."
                );
            }
        }

        private static void checkForMutuallyRequiredKeys(MapValue dataConfig, String firstKey, String secondKey) {
            if (dataConfig.containsKey(firstKey) && !dataConfig.containsKey(secondKey)) {
                throw error(
                    "The configuration key '%1$s' is missing, but '%2$s' is provided. " +
                        "If you really meant to only provide `%2$s` with no value for `%1$s`, " +
                        "you can set `%1$s` to `NULL`.",
                    secondKey,
                    firstKey
                );
            }
        }

        private static IllegalArgumentException error(@PrintFormat String message, Object... args) {
            return new IllegalArgumentException(formatWithLocale(message, args));
        }

        private static Collection<String> mapKeys(MapValue map) {
            var keys = map.keySet();
            if (keys instanceof Collection) {
                return (Collection<String>) keys;
            }
            return StreamSupport.stream(keys.spliterator(), false).collect(Collectors.toSet());
        }
    }
}
