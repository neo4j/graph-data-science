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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.CustomProcedure;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.compat.CompatUserAggregator;
import org.neo4j.gds.core.ConfigKeyValidation;
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.PropertyValues;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.Name;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.gds.projection.GraphImporter.NO_TARGET_NODE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

// public is required for the Cypher runtime
@SuppressWarnings("WeakerAccess")
public class GraphAggregator implements CompatUserAggregator {

    private final DatabaseId databaseId;
    private final String username;
    private final boolean canWriteToDatabase;

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
        boolean canWriteToDatabase
    ) {
        this.databaseId = databaseId;
        this.username = username;
        this.canWriteToDatabase = canWriteToDatabase;
        this.progressTimer = ProgressTimer.start();
        this.lock = new ReentrantLock();
        this.configValidator = new ConfigValidator();
        this.extractNodeId = new ExtractNodeId();
    }

    // NOTE: keep in sync with `CypherAggregation.FUNCTION_NAME`
    @CustomProcedure("gds.alpha.graph.project")
    public AggregationResult procedureSyntax(
        @Name("graphName") TextValue graphName,
        @Name("sourceNode") AnyValue sourceNode,
        @Name("targetNode") AnyValue targetNode,
        @Name("nodesConfig") AnyValue nodesConfig,
        @Name("relationshipConfig") AnyValue relationshipConfig,
        @Name("configuration") AnyValue config
    ) {
        throw new UnsupportedOperationException("This method is only used to document the procedure syntax.");
    }

    @Override
    public void update(AnyValue[] input) throws ProcedureException {
        try {
            this.projectNextRelationship(
                (TextValue) input[0],
                input[1],
                input[2],
                input[3],
                input[4],
                input[5]
            );
        } catch (Exception e) {
            throw new ProcedureException(
                Status.Procedure.ProcedureCallFailed,
                e,
                "Failed to invoke function `%s`: Caused by: %s",
                CypherAggregation.FUNCTION_NAME,
                e
            );
        }
    }

    void projectNextRelationship(
        TextValue graphName,
        AnyValue sourceNode,
        AnyValue targetNode,
        AnyValue nodesConfig,
        AnyValue relationshipConfig,
        AnyValue config
    ) {
        this.configValidator.validateConfigs(nodesConfig, relationshipConfig);

        var data = initGraphData(graphName, config);

        @Nullable PropertyValues sourceNodePropertyValues = null;
        @Nullable PropertyValues targetNodePropertyValues = null;
        NodeLabelToken sourceNodeLabels = NodeLabelTokens.missing();
        NodeLabelToken targetNodeLabels = NodeLabelTokens.missing();

        if (nodesConfig instanceof MapValue) {
            sourceNodePropertyValues = propertiesConfig("sourceNodeProperties", (MapValue) nodesConfig);
            sourceNodeLabels = labelsConfig("sourceNodeLabels", (MapValue) nodesConfig);

            if (targetNode != NoValue.NO_VALUE) {
                targetNodePropertyValues = propertiesConfig("targetNodeProperties", (MapValue) nodesConfig);
                targetNodeLabels = labelsConfig("targetNodeLabels", (MapValue) nodesConfig);
            }
        }

        PropertyValues relationshipProperties = null;
        RelationshipType relationshipType = RelationshipType.ALL_RELATIONSHIPS;

        if (relationshipConfig instanceof MapValue) {
            relationshipProperties = propertiesConfig("properties", (MapValue) relationshipConfig);
            relationshipType = typeConfig("relationshipType", (MapValue) relationshipConfig);
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
                this.importer = data = GraphImporter.of(
                    graphName,
                    this.username,
                    this.databaseId,
                    config,
                    this.canWriteToDatabase
                );
            }
            return data;
        } finally {
            this.lock.unlock();
        }
    }

    private static NodeLabelToken labelsConfig(String nodeLabelKey, @NotNull MapValue nodesConfig) {
        var nodeLabelsEntry = nodesConfig.get(nodeLabelKey);
        return tryLabelsConfig(nodeLabelsEntry, nodeLabelKey);
    }

    private static NodeLabelToken tryLabelsConfig(AnyValue nodeLabels, String nodeLabelKey) {
        var nodeLabelToken = nodeLabels.map(ReadNodeLabels.INSTANCE);

        if (nodeLabelToken.isInvalid()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The value of `%s` must be either a `List of Strings`, a `String`, or a `Boolean`, but was `%s`.",
                nodeLabelKey,
                nodeLabels.getTypeName()
            ));
        }

        return nodeLabelToken;
    }

    @Override
    public AnyValue result() throws ProcedureException {
        AggregationResult result;
        try {
            result = buildGraph();
        } catch (Exception e) {
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

        this.result = importer.result(
            this.username,
            this.databaseId,
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

        throw new IllegalArgumentException(formatWithLocale(
            "The value of `%s` must be a `Map of Property Values`, but was `%s`.",
            key,
            properties.getTypeName()
        ));
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

        throw new IllegalArgumentException(formatWithLocale(
            "The value of `%s` must be `String`, but was `%s`.",
            relationshipTypeKey,
            relationshipTypeEntry.valueRepresentation().valueGroup()
        ));
    }

    private static final class ConfigValidator {
        private static final Set<String> NODES_CONFIG_KEYS = Set.of(
            "sourceNodeProperties",
            "sourceNodeLabels",
            "targetNodeProperties",
            "targetNodeLabels"
        );

        private static final Set<String> RELATIONSHIPS_CONFIG_KEYS = Set.of(
            "properties",
            "relationshipType"
        );


        private final AtomicBoolean validate = new AtomicBoolean(true);

        void validateConfigs(AnyValue nodesConfig, AnyValue relationshipConfig) {
            if (nodesConfig instanceof MapValue || relationshipConfig instanceof MapValue) {
                if (this.validate.get()) {
                    if (this.validate.getAndSet(false)) {
                        if (nodesConfig instanceof MapValue) {
                            ConfigKeyValidation.requireOnlyKeysFrom(
                                NODES_CONFIG_KEYS,
                                ((MapValue) nodesConfig).keySet()
                            );
                        }
                        if (relationshipConfig instanceof MapValue) {
                            ConfigKeyValidation.requireOnlyKeysFrom(
                                RELATIONSHIPS_CONFIG_KEYS,
                                ((MapValue) relationshipConfig).keySet()
                            );
                        }
                    }
                }
            }
        }
    }

}
