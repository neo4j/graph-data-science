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
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.NodeProperties;
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
import org.neo4j.gds.core.utils.paged.HugeAtomicGrowingBitSet;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongPredicate;

import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.gds.core.GraphDimensions.IGNORE;
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

    private final IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyToken;
    private final boolean hasProperties;

    NodesBuilder(
        long maxOriginalId,
        int concurrency,
        ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping,
        IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping,
        IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyKey,
        IdMapBuilder idMapBuilder,
        boolean hasLabelInformation,
        boolean hasProperties,
        boolean deduplicateIds
    ) {
        this.maxOriginalId = maxOriginalId;
        this.concurrency = concurrency;
        this.elementIdentifierLabelTokenMapping = elementIdentifierLabelTokenMapping;
        this.idMapBuilder = idMapBuilder;
        this.labelInformationBuilder = LabelInformation.builder(maxOriginalId + 1);
        this.labelTokenNodeLabelMapping = labelTokenNodeLabelMapping;
        this.nextLabelId = 0;
        this.lock = new ReentrantLock(true);
        this.buildersByLabelTokenAndPropertyToken = buildersByLabelTokenAndPropertyKey;
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
        BiFunction<Integer, String, NodePropertiesFromStoreBuilder> propertyBuilderFn = buildersByLabelTokenAndPropertyKey.isEmpty()
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
                propertyBuilderFn,
                buildersByLabelTokenAndPropertyKey
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

    public void addNode(long originalId, NodeLabel... nodeLabels) {
        this.threadLocalBuilder.get().addNode(originalId, nodeLabels);
    }

    public void addNode(long originalId, Map<String, Value> properties, NodeLabel... nodeLabels) {
        this.threadLocalBuilder.get().addNode(originalId, properties, nodeLabels);
    }

    public void flush() {
        this.threadLocalBuilder.get().flush();
    }

    public long importedNodes() {
        return this.importedNodes.sum();
    }

    public IdMapAndProperties build() {
        return build(maxOriginalId, false);
    }

    public IdMapAndProperties buildChecked() {
        return build(maxOriginalId, true);
    }

    public IdMapAndProperties buildChecked(long highestNeoId) {
        return build(highestNeoId, true);
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

    public IdMapAndProperties build(long highestNeoId, boolean checkDuplicateIds) {
        // Flush remaining buffer contents
        this.threadLocalBuilder.forEach(ThreadLocalBuilder::flush);
        // Clean up resources held by local builders
        this.threadLocalBuilder.close();

        var idMap = this.idMapBuilder.build(
            labelInformationBuilder,
            highestNeoId,
            concurrency,
            checkDuplicateIds
        );

        Optional<Map<NodeLabel, Map<String, NodeProperties>>> nodeProperties = Optional.empty();
        if (hasProperties) {
            nodeProperties = Optional.of(buildProperties(idMap));
        }
        return ImmutableIdMapAndProperties.of(idMap, nodeProperties);
    }

    private Map<NodeLabel, Map<String, NodeProperties>> buildProperties(IdMap idMap) {
        Map<NodeLabel, Map<String, NodeProperties>> nodePropertiesByLabel = new HashMap<>();
        for (IntObjectCursor<Map<String, NodePropertiesFromStoreBuilder>> propertyBuilderByLabelToken : this.buildersByLabelTokenAndPropertyToken) {
            var nodeLabels = labelTokenNodeLabelMapping.get(propertyBuilderByLabelToken.key);
            nodeLabels.forEach(nodeLabel ->
                propertyBuilderByLabelToken.value.forEach((propertyKey, propertyBuilder) -> nodePropertiesByLabel
                    .computeIfAbsent(nodeLabel, __ -> new HashMap<>())
                    .put(propertyKey, propertyBuilder.build(idMap))
                )
            );
        }
        return nodePropertiesByLabel;
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

    private NodePropertiesFromStoreBuilder getOrCreatePropertyBuilder(int labelId, String propertyKey) {
        if (!buildersByLabelTokenAndPropertyToken.containsKey(labelId)) {
            buildersByLabelTokenAndPropertyToken.put(labelId, new HashMap<>());
        }
        var propertyBuildersByPropertyKey = buildersByLabelTokenAndPropertyToken.get(labelId);
        if (!propertyBuildersByPropertyKey.containsKey(propertyKey)) {
            propertyBuildersByPropertyKey.put(
                propertyKey,
                NodePropertiesFromStoreBuilder.of(NO_PROPERTY_VALUE, concurrency)
            );
        }
        return propertyBuildersByPropertyKey.get(propertyKey);
    }

    private NodePropertiesFromStoreBuilder getPropertyBuilder(int labelId, String propertyKey) {
        if (buildersByLabelTokenAndPropertyToken.containsKey(labelId)) {
            Map<String, NodePropertiesFromStoreBuilder> propertyBuildersByPropertyKey = buildersByLabelTokenAndPropertyToken.get(labelId);
            if (propertyBuildersByPropertyKey.containsKey(propertyKey)) {
                return propertyBuildersByPropertyKey.get(propertyKey);
            }
        }
        return null;
    }

    @ValueClass
    public interface IdMapAndProperties {
        IdMap idMap();

        Optional<Map<NodeLabel, Map<String, NodeProperties>>> nodeProperties();
    }

    private static class ThreadLocalBuilder implements AutoCloseable {

        private final long[] anyLabelArray = {ANY_LABEL};

        private final LongAdder importedNodes;
        private final LongPredicate seenNodeIdPredicate;
        private final NodesBatchBuffer buffer;
        private final Function<NodeLabel, Integer> labelTokenIdFn;
        private final BiFunction<Integer, String, NodePropertiesFromStoreBuilder> propertyBuilderFn;
        private final NodeImporter nodeImporter;
        private final IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyKey;
        private final List<Map<String, Value>> batchNodeProperties;

        private boolean isClosed = false;

        ThreadLocalBuilder(
            LongAdder importedNodes,
            NodeImporter nodeImporter,
            long highestPossibleNodeCount,
            LongPredicate seenNodeIdPredicate,
            boolean hasLabelInformation,
            boolean hasProperties,
            Function<NodeLabel, Integer> labelTokenIdFn,
            BiFunction<Integer, String, NodePropertiesFromStoreBuilder> propertyBuilderFn,
            IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyKey
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
            this.buildersByLabelTokenAndPropertyKey = buildersByLabelTokenAndPropertyKey;
            this.batchNodeProperties = new ArrayList<>(buffer.capacity());
        }

        public void addNode(long originalId, NodeLabel... nodeLabels) {
            if (!seenNodeIdPredicate.test(originalId)) {
                long[] labels = labelTokens(nodeLabels);

                buffer.add(originalId, LongPropertyReference.empty(), labels);
                if (buffer.isFull()) {
                    flushBuffer();
                    reset();
                }
            }
        }

        public void flush() {
            flushBuffer();
            reset();
        }

        public void addNode(long originalId, Map<String, Value> properties, NodeLabel... nodeLabels) {
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

        private long[] labelTokens(NodeLabel... nodeLabels) {
            if (nodeLabels == null || nodeLabels.length == 0) {
                anyLabelArray[0] = labelTokenIdFn.apply(NodeLabel.ALL_NODES);
                return anyLabelArray;
            }

            long[] labelIds = new long[nodeLabels.length];

            for (int i = 0; i < nodeLabels.length; i++) {
                labelIds[i] = labelTokenIdFn.apply(nodeLabels[i]);
            }

            return labelIds;
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
                            labelIds,
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

        private int importProperty(long neoNodeId, long[] labels, String propertyKey, Value value) {
            int propertiesImported = 0;

            for (long label : labels) {
                if (label == IGNORE || label == ANY_LABEL) {
                    continue;
                }

                var nodePropertyBuilder = propertyBuilderFn.apply((int) label, propertyKey);
                if (nodePropertyBuilder != null) {
                    nodePropertyBuilder.set(neoNodeId, value);
                    propertiesImported++;
                }
            }

            if (buildersByLabelTokenAndPropertyKey.containsKey(ANY_LABEL)) {
                var nodePropertiesBuilder = buildersByLabelTokenAndPropertyKey
                    .get(ANY_LABEL)
                    .get(propertyKey);
                if (nodePropertiesBuilder != null) {
                    nodePropertiesBuilder.set(neoNodeId, value);
                    propertiesImported++;
                }
            }

            return propertiesImported;
        }
    }
}
