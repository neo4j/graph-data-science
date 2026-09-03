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
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.PropertyValues;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.UserAggregationUpdater;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.gds.projection.CypherAggregation.FUNCTION_NAME;
import static org.neo4j.gds.projection.GraphImporter.NO_TARGET_NODE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CypherAggregationUpdater implements UserAggregationUpdater, AutoCloseable {


    static final String SOURCE_NODE_PROPERTIES = "sourceNodeProperties";
    static final String SOURCE_NODE_LABELS = "sourceNodeLabels";
    static final String TARGET_NODE_PROPERTIES = "targetNodeProperties";
    static final String TARGET_NODE_LABELS = "targetNodeLabels";
    static final String ALPHA_RELATIONSHIP_PROPERTIES = "properties";
    static final String RELATIONSHIP_PROPERTIES = "relationshipProperties";
    static final String RELATIONSHIP_TYPE = "relationshipType";

    private final LazyGraphImporter lazyGraphImporter;

    private final ExtractNodeId extractNodeId;
    private final InputValuesMapper inputValuesMapper;
    private final ConfigValidator configValidator;

    private volatile @Nullable GraphImporter.ThreadLocalBatches threadLocalBatches;

    public interface InputValuesMapper {
        AnyValue[] map(AnyValue[] from);

        static InputValuesMapper identity() {
            return from -> from;
        }
    }

    public CypherAggregationUpdater(
        LazyGraphImporter lazyGraphImporter,
        ExtractNodeId extractNodeId,
        InputValuesMapper inputValuesMapper
    ) {
        this.lazyGraphImporter = lazyGraphImporter;
        this.extractNodeId = extractNodeId;
        this.inputValuesMapper = inputValuesMapper;
        this.configValidator = new ConfigValidator();
    }

    @Override
    public void update(AnyValue[] input) throws ProcedureException {
        try {
            var mappedInput = inputValuesMapper.map(input);
            projectNextRelationship(
                (TextValue) mappedInput[0],
                mappedInput[1],
                mappedInput[2],
                mappedInput[3],
                mappedInput[4],
                mappedInput[5]
            );
        } catch (Throwable t) {
            throw ProcedureException.invocationFailed("function", FUNCTION_NAME.toString(), t);
        }
    }

    @Override
    public void applyUpdates() {
        var session = this.threadLocalBatches;
        if (session != null) {
            session.releaseBatches();
        }
    }

    GraphImporter.ThreadLocalBatches getThreadLocalBatches(GraphImporter graphImporter) {
        if (threadLocalBatches == null) {
            threadLocalBatches = graphImporter.newThreadLocalBatches();
        }
        return threadLocalBatches;
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

        var importer = lazyGraphImporter.initializeImporter(graphName, config, dataConfig, migrationConfig);

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

        importer.update(
            getThreadLocalBatches(importer),
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

    @Override
    public void close() {
        if (threadLocalBatches != null) {
            threadLocalBatches.close();
        }
        lazyGraphImporter.close();
    }

    @Nullable
    private static PropertyValues propertiesConfig(String key, @NotNull MapValue container) {
        var properties = container.get(key);
        if (properties instanceof MapValue mapProperties) {
            if (mapProperties.isEmpty()) return null;

            return new CypherPropertyValues(mapProperties);
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

    private long extractNodeId(@NotNull AnyValue node) {
        return node.map(this.extractNodeId);
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

}
