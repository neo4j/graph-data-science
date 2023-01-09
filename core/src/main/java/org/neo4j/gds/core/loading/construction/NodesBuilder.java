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

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.compat.LongPropertyReference;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.LabelInformation;
import org.neo4j.gds.core.loading.NodeImporter;
import org.neo4j.gds.core.loading.NodesBatchBuffer;
import org.neo4j.gds.core.loading.NodesBatchBufferBuilder;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.gds.core.utils.RawValues;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicPagedBitSet;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.LongPredicate;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.core.GraphDimensions.NO_SUCH_LABEL;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodesBuilder {

    public static final DefaultValue NO_PROPERTY_VALUE = DefaultValue.DEFAULT;
    public static final long UNKNOWN_MAX_ID = -1L;

    private final long maxOriginalId;
    private final int concurrency;

    private int nextLabelId;
    private final ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping;
    private final IdMapBuilder idMapBuilder;
    private final LabelInformation.Builder labelInformationBuilder;
    private final IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping;

    private final LongAdder importedNodes;
    private final AutoCloseableThreadLocal<ThreadLocalBuilder> threadLocalBuilder;

    private final NodeImporter nodeImporter;

    private final Lock lock;

    private final ConcurrentMap<String, NodePropertiesFromStoreBuilder> propertyBuildersByPropertyKey;
    private final boolean hasProperties;

    NodesBuilder(
        long maxOriginalId,
        int concurrency,
        ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping,
        IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping,
        ConcurrentMap<String, NodePropertiesFromStoreBuilder> propertyBuildersByPropertyKey,
        IdMapBuilder idMapBuilder,
        boolean hasLabelInformation,
        boolean hasProperties,
        boolean deduplicateIds
    ) {
        this.maxOriginalId = maxOriginalId;
        this.concurrency = concurrency;
        this.elementIdentifierLabelTokenMapping = elementIdentifierLabelTokenMapping;
        this.idMapBuilder = idMapBuilder;
        this.labelInformationBuilder = !hasLabelInformation
            ? LabelInformation.single(NodeLabel.ALL_NODES)
            : LabelInformation.builder(maxOriginalId + 1);
        this.labelTokenNodeLabelMapping = labelTokenNodeLabelMapping;
        this.nextLabelId = 0;
        this.lock = new ReentrantLock(true);
        this.propertyBuildersByPropertyKey = propertyBuildersByPropertyKey;
        this.hasProperties = hasProperties;
        this.importedNodes = new LongAdder();
        this.nodeImporter = new NodeImporter(
            idMapBuilder,
            labelInformationBuilder,
            labelTokenNodeLabelMapping,
            hasProperties
        );

        Function<NodeLabel, Integer> labelTokenIdFn = elementIdentifierLabelTokenMapping.isEmpty()
            ? this::getOrCreateLabelTokenId
            : this::getLabelTokenId;
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
                labelTokenIdFn,
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
                var seenIds = HugeAtomicPagedBitSet.create(0);
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
        this.threadLocalBuilder.get().addNode(originalId, properties, nodeLabels);
    }

    public void addNode(long originalId, Map<String, Value> properties, NodeLabel... nodeLabels) {
        this.addNode(originalId, properties, NodeLabelTokens.ofNodeLabels(nodeLabels));
    }

    public void addNode(long originalId, Map<String, Value> properties, NodeLabel nodeLabel) {
        this.addNode(originalId, properties, NodeLabelTokens.ofNodeLabel(nodeLabel));
    }

    public long importedNodes() {
        return this.importedNodes.sum();
    }

    public IdMapAndProperties build() {
        return build(maxOriginalId);
    }

    public IdMapAndProperties build(long highestNeoId) {
        // Flush remaining buffer contents
        this.threadLocalBuilder.forEach(ThreadLocalBuilder::flush);
        // Clean up resources held by local builders
        this.threadLocalBuilder.close();

        var idMap = this.idMapBuilder.build(labelInformationBuilder, highestNeoId, concurrency);

        Optional<Map<String, NodePropertyValues>> nodeProperties = Optional.empty();
        if (hasProperties) {
            nodeProperties = Optional.of(buildProperties(idMap));
        }
        return ImmutableIdMapAndProperties.of(idMap, nodeProperties);
    }

    private Map<String, NodePropertyValues> buildProperties(IdMap idMap) {
        return propertyBuildersByPropertyKey.entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            e -> e.getValue().build(idMap)
        ));
    }

    /**
     * Closes the NodesBuilder without flushing the internal buffers.
     * The given exception is thrown, once the thread local builders
     * are closed.
     *
     * This method must be called in case of an error while using the
     * NodesBuilder.
     */
    public void close(RuntimeException exception) {
        this.threadLocalBuilder.close();
        throw exception;
    }

    private int getOrCreateLabelTokenId(NodeLabel nodeLabel) {
        var token = elementIdentifierLabelTokenMapping.getOrDefault(nodeLabel, NO_SUCH_LABEL);
        if (token == NO_SUCH_LABEL) {
            lock.lock();
            token = elementIdentifierLabelTokenMapping.getOrDefault(nodeLabel, NO_SUCH_LABEL);
            if (token == NO_SUCH_LABEL) {
                token = nextLabelId++;
                labelTokenNodeLabelMapping.put(token, Collections.singletonList(nodeLabel));
                elementIdentifierLabelTokenMapping.put(nodeLabel, token);
            }
            lock.unlock();
        }
        return token;
    }

    private int getLabelTokenId(NodeLabel nodeLabel) {
        if (!elementIdentifierLabelTokenMapping.containsKey(nodeLabel)) {
            throw new IllegalArgumentException(formatWithLocale("No token was specified for node label %s", nodeLabel));
        }
        return elementIdentifierLabelTokenMapping.get(nodeLabel);
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
        private final NodesBatchBuffer buffer;
        private final Function<NodeLabel, Integer> labelTokenIdFn;
        private final Function<String, NodePropertiesFromStoreBuilder> propertyBuilderFn;
        private final NodeImporter nodeImporter;
        private final List<Map<String, Value>> batchNodeProperties;

        ThreadLocalBuilder(
            LongAdder importedNodes,
            NodeImporter nodeImporter,
            long highestPossibleNodeCount,
            LongPredicate seenNodeIdPredicate,
            boolean hasLabelInformation,
            boolean hasProperties,
            Function<NodeLabel, Integer> labelTokenIdFn,
            Function<String, NodePropertiesFromStoreBuilder> propertyBuilderFn
        ) {
            this.importedNodes = importedNodes;
            this.seenNodeIdPredicate = seenNodeIdPredicate;
            this.labelTokenIdFn = labelTokenIdFn;
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
                long[] labels = labelTokens(nodeLabels);

                buffer.add(originalId, LongPropertyReference.empty(), labels);
                if (buffer.isFull()) {
                    flushBuffer();
                    reset();
                }
            }
        }

        public void addNode(long originalId, Map<String, Value> properties, NodeLabelToken nodeLabels) {
            if (!seenNodeIdPredicate.test(originalId)) {
                long[] labels = labelTokens(nodeLabels);

                int propertyReference = batchNodeProperties.size();
                batchNodeProperties.add(properties);

                buffer.add(originalId, LongPropertyReference.of(propertyReference), labels);
                if (buffer.isFull()) {
                    flushBuffer();
                    reset();
                }
            }
        }

        private long[] labelTokens(NodeLabelToken nodeLabels) {
            if (nodeLabels.isEmpty()) {
                return anyLabelArray();
            }

            long[] labelIds = new long[nodeLabels.size()];
            for (int i = 0; i < labelIds.length; i++) {
                labelIds[i] = labelTokenIdFn.apply(nodeLabels.get(i));
            }

            return labelIds;
        }

        public void flush() {
            flushBuffer();
            reset();
        }

        private void flushBuffer() {
            var importedNodesAndProperties = this.nodeImporter.importNodes(
                buffer,
                (nodeReference, labelIds, propertiesReference) -> {
                    if (!propertiesReference.isEmpty()) {
                        var propertyValueIndex = (int) ((LongPropertyReference) propertiesReference).id;
                        Map<String, Value> properties = batchNodeProperties.get(propertyValueIndex);
                        MutableInt importedProperties = new MutableInt(0);
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
                anyLabelArray[0] = labelTokenIdFn.apply(NodeLabel.ALL_NODES);
            }
            return anyLabelArray;
        }
    }
}
