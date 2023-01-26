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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.ImmutableNodeProperty;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.compat.LongPropertyReference;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.ImmutableNodes;
import org.neo4j.gds.core.loading.LabelInformation;
import org.neo4j.gds.core.loading.LabelInformationBuilders;
import org.neo4j.gds.core.loading.NodeImporter;
import org.neo4j.gds.core.loading.NodeImporterBuilder;
import org.neo4j.gds.core.loading.Nodes;
import org.neo4j.gds.core.loading.NodesBatchBuffer;
import org.neo4j.gds.core.loading.NodesBatchBufferBuilder;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.RawValues;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicGrowingBitSet;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.MapValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public final class NodesBuilder {

    private static final DefaultValue NO_PROPERTY_VALUE = DefaultValue.DEFAULT;
    public static final long UNKNOWN_MAX_ID = -1L;

    private final long maxOriginalId;
    private final int concurrency;

    private final IdMapBuilder idMapBuilder;
    private final Function<String, PropertyState> propertyStates;
    private final LabelInformation.Builder labelInformationBuilder;

    private final LongAdder importedNodes;
    private final AutoCloseableThreadLocal<ThreadLocalBuilder> threadLocalBuilder;

    private final NodeImporter nodeImporter;

    private final ConcurrentMap<String, NodePropertiesFromStoreBuilder> propertyBuildersByPropertyKey;

    NodesBuilder(
        long maxOriginalId,
        int concurrency,
        Supplier<TokenToNodeLabels> tokenToNodeLabelSupplier,
        Supplier<NodeLabelTokenToPropertyKeys> nodeLabelTokenToPropertyKeysSupplier,
        ConcurrentMap<String, NodePropertiesFromStoreBuilder> propertyBuildersByPropertyKey,
        IdMapBuilder idMapBuilder,
        boolean hasLabelInformation,
        boolean hasProperties,
        boolean deduplicateIds,
        Function<String, PropertyState> propertyStates
    ) {
        this.maxOriginalId = maxOriginalId;
        this.concurrency = concurrency;
        this.idMapBuilder = idMapBuilder;
        this.propertyStates = propertyStates;
        this.labelInformationBuilder = !hasLabelInformation
            ? LabelInformationBuilders.allNodes()
            : LabelInformationBuilders.multiLabelWithCapacity(maxOriginalId + 1);
        this.propertyBuildersByPropertyKey = propertyBuildersByPropertyKey;
        this.importedNodes = new LongAdder();
        this.nodeImporter = new NodeImporterBuilder()
            .idMapBuilder(idMapBuilder)
            .labelInformationBuilder(labelInformationBuilder)
            .importProperties(hasProperties)
            .build();

        Function<String, NodePropertiesFromStoreBuilder> propertyBuilderFn = propertyBuildersByPropertyKey.isEmpty()
            ? this::getOrCreatePropertyBuilder
            : this::getPropertyBuilder;
        LongPredicate seenNodeIdPredicate = seenNodesPredicate(deduplicateIds, maxOriginalId);
        long highestPossibleNodeCount = maxOriginalId == UNKNOWN_MAX_ID
            ? Long.MAX_VALUE
            : maxOriginalId + 1;
        this.threadLocalBuilder = AutoCloseableThreadLocal.withInitial(
            () -> new NodesBuilder.ThreadLocalBuilder(
                importedNodes,
                nodeImporter,
                highestPossibleNodeCount,
                seenNodeIdPredicate,
                hasLabelInformation,
                hasProperties,
                tokenToNodeLabelSupplier.get(),
                nodeLabelTokenToPropertyKeysSupplier.get(),
                propertyBuilderFn
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
        this.threadLocalBuilder.get().addNode(originalId, nodeLabels);
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
        this.threadLocalBuilder.get().addNode(originalId, nodeLabels, properties);
    }

    public long importedNodes() {
        return this.importedNodes.sum();
    }

    public Nodes build() {
        return build(maxOriginalId);
    }

    public Nodes build(long highestNeoId) {
        var localLabelTokenToPropertyKeys = closeThreadLocalBuilders();

        var idMap = this.idMapBuilder.build(labelInformationBuilder, highestNeoId, concurrency);
        var nodeProperties = buildProperties(idMap);
        var nodeSchema = buildNodeSchema(idMap, localLabelTokenToPropertyKeys, nodeProperties);
        var nodePropertyStore = NodePropertyStore.builder().properties(nodeProperties).build();

        return ImmutableNodes.builder()
            .schema(nodeSchema)
            .idMap(idMap)
            .properties(nodePropertyStore)
            .build();
    }

    private List<NodeLabelTokenToPropertyKeys> closeThreadLocalBuilders() {
        // Flush remaining buffer contents
        this.threadLocalBuilder.forEach(ThreadLocalBuilder::flush);
        // Collect token to property keys for final union
        var labelTokenToPropertyKeys = new ArrayList<NodeLabelTokenToPropertyKeys>();
        this.threadLocalBuilder.forEach(tlb -> labelTokenToPropertyKeys.add(tlb.nodeLabelTokenToPropertyKeys));
        // Clean up resources held by local builders
        this.threadLocalBuilder.close();

        return labelTokenToPropertyKeys;
    }

    private MutableNodeSchema buildNodeSchema(
        IdMap idMap,
        Collection<NodeLabelTokenToPropertyKeys> localLabelTokenToPropertyKeys,
        Map<String, NodeProperty> nodeProperties
    ) {

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
        return propertyBuildersByPropertyKey.entrySet().stream().collect(toMap(
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

    /**
     * Closes the NodesBuilder without flushing the internal buffers.
     * The given exception is thrown, once the thread local builders
     * are closed.
     * <p>
     * This method must be called in case of an error while using the
     * NodesBuilder.
     */
    public void close(RuntimeException exception) {
        this.threadLocalBuilder.close();
        throw exception;
    }

    private NodePropertiesFromStoreBuilder getOrCreatePropertyBuilder(String propertyKey) {
        return propertyBuildersByPropertyKey.computeIfAbsent(
            propertyKey,
            __ -> NodePropertiesFromStoreBuilder.of(NO_PROPERTY_VALUE, concurrency)
        );
    }

    private NodePropertiesFromStoreBuilder getPropertyBuilder(String propertyKey) {
        return propertyBuildersByPropertyKey.get(propertyKey);
    }

    @ValueClass
    public interface IdMapAndProperties {
        IdMap idMap();

        Optional<Map<String, NodePropertyValues>> nodeProperties();
    }

    private static class ThreadLocalBuilder implements AutoCloseable {

        private static final long NOT_INITIALIZED = -42L;
        private final long[] anyLabelArray = {NOT_INITIALIZED};

        private final LongAdder importedNodes;
        private final LongPredicate seenNodeIdPredicate;
        private final TokenToNodeLabels tokenToNodeLabels;
        private final NodeLabelTokenToPropertyKeys nodeLabelTokenToPropertyKeys;
        private final NodesBatchBuffer buffer;
        private final Function<String, NodePropertiesFromStoreBuilder> propertyBuilderFn;
        private final NodeImporter nodeImporter;
        private final List<PropertyValues> batchNodeProperties;

        ThreadLocalBuilder(
            LongAdder importedNodes,
            NodeImporter nodeImporter,
            long highestPossibleNodeCount,
            LongPredicate seenNodeIdPredicate,
            boolean hasLabelInformation,
            boolean hasProperties,
            TokenToNodeLabels tokenToNodeLabels,
            NodeLabelTokenToPropertyKeys nodeLabelTokenToPropertyKeys,
            Function<String, NodePropertiesFromStoreBuilder> propertyBuilderFn
        ) {
            this.importedNodes = importedNodes;
            this.seenNodeIdPredicate = seenNodeIdPredicate;
            this.tokenToNodeLabels = tokenToNodeLabels;
            this.nodeLabelTokenToPropertyKeys = nodeLabelTokenToPropertyKeys;
            this.propertyBuilderFn = propertyBuilderFn;

            this.buffer = new NodesBatchBufferBuilder()
                .capacity(ParallelUtil.DEFAULT_BATCH_SIZE)
                .highestPossibleNodeCount(highestPossibleNodeCount)
                .hasLabelInformation(hasLabelInformation)
                .readProperty(hasProperties)
                .build();
            this.nodeImporter = nodeImporter;
            this.batchNodeProperties = new ArrayList<>(buffer.capacity());
        }

        public void addNode(long originalId, NodeLabelToken nodeLabels) {
            if (!seenNodeIdPredicate.test(originalId)) {
                long[] labels = getOrCreateLabelTokens(nodeLabels);

                buffer.add(originalId, LongPropertyReference.empty(), labels);
                if (buffer.isFull()) {
                    flushBuffer();
                    reset();
                }
            }
        }

        public void addNode(long originalId, NodeLabelToken nodeLabels, PropertyValues properties) {
            if (!seenNodeIdPredicate.test(originalId)) {
                long[] labels = getOrCreateLabelTokens(nodeLabels);
                this.nodeLabelTokenToPropertyKeys.add(nodeLabels, properties.propertyKeys());

                int propertyReference = batchNodeProperties.size();
                batchNodeProperties.add(properties);

                buffer.add(originalId, LongPropertyReference.of(propertyReference), labels);
                if (buffer.isFull()) {
                    flushBuffer();
                    reset();
                }
            }
        }

        private long[] getOrCreateLabelTokens(NodeLabelToken nodeLabels) {
            if (nodeLabels.isEmpty()) {
                return anyLabelArray();
            }

            long[] labelIds = new long[nodeLabels.size()];
            for (int i = 0; i < labelIds.length; i++) {
                labelIds[i] = this.tokenToNodeLabels.getOrCreateToken(nodeLabels.get(i));
            }

            return labelIds;
        }

        public void flush() {
            flushBuffer();
            reset();
        }

        private void flushBuffer() {
            var importedNodesAndProperties = this.nodeImporter.importNodes(
                this.buffer,
                this.tokenToNodeLabels.labelTokenNodeLabelMapping(),
                (nodeReference, labelIds, propertiesReference) -> {
                    if (!propertiesReference.isEmpty()) {
                        var propertyValueIndex = (int) ((LongPropertyReference) propertiesReference).id;
                        var properties = batchNodeProperties.get(propertyValueIndex);
                        var importedProperties = new MutableInt(0);
                        properties.forEach((propertyKey, propertyValue) -> importedProperties.add(importProperty(
                            nodeReference,
                            propertyKey,
                            propertyValue
                        )));
                        return importedProperties.intValue();
                    }
                    return 0;
                }
            );
            int importedNodes = RawValues.getHead(importedNodesAndProperties);
            this.importedNodes.add(importedNodes);
        }

        private void reset() {
            buffer.reset();
            batchNodeProperties.clear();
        }

        @Override
        public void close() {}

        private int importProperty(long neoNodeId, String propertyKey, Value value) {
            int propertiesImported = 0;

            var nodePropertyBuilder = propertyBuilderFn.apply(propertyKey);
            if (nodePropertyBuilder != null) {
                nodePropertyBuilder.set(neoNodeId, value);
                propertiesImported++;
            }

            return propertiesImported;
        }

        private long[] anyLabelArray() {
            var anyLabelArray = this.anyLabelArray;
            if (anyLabelArray[0] == NOT_INITIALIZED) {
                anyLabelArray[0] = tokenToNodeLabels.getOrCreateToken(NodeLabel.ALL_NODES);
            }
            return anyLabelArray;
        }
    }
}
