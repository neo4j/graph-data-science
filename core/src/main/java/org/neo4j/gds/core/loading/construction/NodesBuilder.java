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
package org.neo4j.gds.core.loading.construction;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.ImmutableNodeProperty;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.ImmutableNodes;
import org.neo4j.gds.core.loading.LabelInformation;
import org.neo4j.gds.core.loading.LabelInformationBuilders;
import org.neo4j.gds.core.loading.NodeImporter;
import org.neo4j.gds.core.loading.NodeImporterBuilder;
import org.neo4j.gds.core.loading.Nodes;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicGrowingBitSet;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.MapValue;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public final class NodesBuilder {
    static final Integer NO_PROPERTY = -1;
    public static final long UNKNOWN_MAX_ID = -1L;

    private final long maxOriginalId;
    private final Concurrency concurrency;

    private final IdMapBuilder idMapBuilder;
    private final Function<String, PropertyState> propertyStates;
    private final LabelInformation.Builder labelInformationBuilder;

    private final LongAdder importedNodes;
    private final LocalNodesBuilderProvider localNodesBuilderProvider;

    private final NodeImporter nodeImporter;

    private final NodesBuilderContext nodesBuilderContext;

    NodesBuilder(
        long maxOriginalId,
        long maxIntermediateId,
        Concurrency concurrency,
        NodesBuilderContext nodesBuilderContext,
        IdMapBuilder idMapBuilder,
        boolean hasLabelInformation,
        boolean hasProperties,
        boolean deduplicateIds,
        Function<String, PropertyState> propertyStates
    ) {
        this.maxOriginalId = maxOriginalId;
        this.concurrency = concurrency;
        this.nodesBuilderContext = nodesBuilderContext;
        this.idMapBuilder = idMapBuilder;
        this.propertyStates = propertyStates;
        this.labelInformationBuilder = !hasLabelInformation
            ? LabelInformationBuilders.allNodes()
            : LabelInformationBuilders.multiLabelWithCapacity(maxIntermediateId + 1);

        this.importedNodes = new LongAdder();
        this.nodeImporter = new NodeImporterBuilder()
            .idMapBuilder(idMapBuilder)
            .labelInformationBuilder(labelInformationBuilder)
            .importProperties(hasProperties)
            .build();

        LongPredicate seenNodeIdPredicate = seenNodesPredicate(deduplicateIds, maxOriginalId);

        this.localNodesBuilderProvider = LocalNodesBuilderProvider.threadLocal(
            () -> new LocalNodesBuilder(
                importedNodes,
                nodeImporter,
                seenNodeIdPredicate,
                hasLabelInformation,
                hasProperties,
                nodesBuilderContext.threadLocalContext()
            )
        );
    }

    private static LongPredicate seenNodesPredicate(
        boolean deduplicateIds,
        long maxOriginalId
    ) {
        if (deduplicateIds) {
            if (maxOriginalId == UNKNOWN_MAX_ID) {
                var seenIds = HugeAtomicGrowingBitSet.create(0);
                return seenIds::getAndSet;
            } else {
                var seenIds = HugeAtomicBitSet.create(maxOriginalId + 1);
                return seenIds::getAndSet;
            }
        } else {
            return nodeId -> false;
        }
    }

    public void addNode(long originalId) {
        this.addNode(originalId, NodeLabelTokens.empty());
    }

    public void addNode(long originalId, NodeLabelToken nodeLabels) {
        var slot = this.localNodesBuilderProvider.acquire();
        try {
            slot.get().addNode(originalId, nodeLabels);
        } finally {
            slot.release();
        }
    }

    public void addNode(long originalId, NodeLabel... nodeLabels) {
        this.addNode(originalId, NodeLabelTokens.ofNodeLabels(nodeLabels));
    }

    public void addNode(long originalId, NodeLabel nodeLabel) {
        this.addNode(originalId, NodeLabelTokens.ofNodeLabel(nodeLabel));
    }

    public void addNode(long originalId, Map<String, Value> properties) {
        this.addNode(originalId, properties, NodeLabelTokens.empty());
    }

    public void addNode(long originalId, Map<String, Value> properties, NodeLabelToken nodeLabels) {
        this.addNode(originalId, nodeLabels, PropertyValues.of(properties));
    }

    public void addNode(long originalId, MapValue properties, NodeLabelToken nodeLabels) {
        this.addNode(originalId, nodeLabels, PropertyValues.of(properties));
    }

    public void addNode(long originalId, Map<String, Value> properties, NodeLabel... nodeLabels) {
        this.addNode(originalId, properties, NodeLabelTokens.ofNodeLabels(nodeLabels));
    }

    public void addNode(long originalId, Map<String, Value> properties, NodeLabel nodeLabel) {
        this.addNode(originalId, properties, NodeLabelTokens.ofNodeLabel(nodeLabel));
    }

    public void addNode(long originalId, NodeLabelToken nodeLabels, PropertyValues properties) {
        var slot = this.localNodesBuilderProvider.acquire();
        try {
            slot.get().addNode(originalId, nodeLabels, properties);
        } finally {
            slot.release();
        }
    }

    public long importedNodes() {
        return this.importedNodes.sum();
    }

    public Nodes build() {
        return build(maxOriginalId);
    }

    public Nodes build(long highestNeoId) {
        // Flush remaining buffer contents
        this.localNodesBuilderProvider.close();

        var idMap = this.idMapBuilder.build(labelInformationBuilder, highestNeoId, concurrency);
        var nodeProperties = buildProperties(idMap);
        var nodeSchema = buildNodeSchema(idMap, nodeProperties);
        var nodePropertyStore = NodePropertyStore.builder().properties(nodeProperties).build();

        return ImmutableNodes.builder()
            .schema(nodeSchema)
            .idMap(idMap)
            .properties(nodePropertyStore)
            .build();
    }

    private MutableNodeSchema buildNodeSchema(
        IdMap idMap,
        Map<String, NodeProperty> nodeProperties
    ) {
        var localLabelTokenToPropertyKeys = this.nodesBuilderContext.nodeLabelTokenToPropertyKeys();

        // Collect the property schemas from the imported property values.
        var propertyKeysToSchema = nodeProperties
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().propertySchema()));
        // Union the label to property key mappings from each import thread.
        var globalLabelTokenToPropertyKeys = localLabelTokenToPropertyKeys
            .stream()
            .reduce(
                NodeLabelTokenToPropertyKeys.lazy(),
                (left, right) -> NodeLabelTokenToPropertyKeys.union(left, right, propertyKeysToSchema)
            );
        // Collect node labels without properties from the id map
        // as they are not stored in the above union mapping.
        var nodeLabels = new HashSet<>(idMap.availableNodeLabels());
        // Add labels that actually have node properties attached.
        localLabelTokenToPropertyKeys.forEach(localMapping -> nodeLabels.addAll(localMapping.nodeLabels()));

        // Use all labels and the global label to property
        // key mapping to construct the final node schema.
        return nodeLabels.stream()
            .reduce(
                MutableNodeSchema.empty(),
                (unionSchema, nodeLabel) -> unionSchema.addLabel(
                    nodeLabel,
                    globalLabelTokenToPropertyKeys.propertySchemas(nodeLabel, propertyKeysToSchema)
                ),
                (lhs, rhs) -> lhs
            );
    }

    private Map<String, NodeProperty> buildProperties(IdMap idMap) {
        return this.nodesBuilderContext.nodePropertyBuilders().entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            entry -> entryToNodeProperty(entry, propertyStates.apply(entry.getKey()), idMap)
        ));
    }

    private static NodeProperty entryToNodeProperty(
        Map.Entry<String, NodePropertiesFromStoreBuilder> entry,
        PropertyState propertyState,
        IdMap idMap
    ) {
        var nodePropertyValues = entry.getValue().build(idMap);
        var valueType = nodePropertyValues.valueType();
        return ImmutableNodeProperty.builder()
            .values(nodePropertyValues)
            .propertySchema(PropertySchema.of(entry.getKey(), valueType, valueType.fallbackValue(), propertyState))
            .build();
    }

    public void close(RuntimeException exception) {
        this.localNodesBuilderProvider.close();
        throw exception;
    }
}
