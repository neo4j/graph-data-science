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
package org.neo4j.graphalgo.core.loading.construction;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.UnionNodeProperties;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.loading.IdMappingAllocator;
import org.neo4j.graphalgo.core.loading.InternalIdMappingBuilder;
import org.neo4j.graphalgo.core.loading.NodeImporter;
import org.neo4j.graphalgo.core.loading.NodeMappingBuilder;
import org.neo4j.graphalgo.core.loading.NodesBatchBuffer;
import org.neo4j.graphalgo.core.loading.NodesBatchBufferBuilder;
import org.neo4j.graphalgo.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;
import org.neo4j.graphalgo.utils.AutoCloseableThreadLocal;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.graphalgo.core.GraphDimensions.IGNORE;
import static org.neo4j.graphalgo.core.GraphDimensions.NO_SUCH_LABEL;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public final class NodesBuilder {

    public static final DefaultValue NO_PROPERTY_VALUE = DefaultValue.DEFAULT;
    public static final long UNKNOWN_MAX_ID = -1L;

    private final long maxOriginalId;
    private final long nodeCount;
    private final int concurrency;
    private final AllocationTracker tracker;

    private int nextLabelId;
    private final ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping;
    private final Map<NodeLabel, HugeAtomicBitSet> nodeLabelBitSetMap;
    private final IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping;

    private final AutoCloseableThreadLocal<ThreadLocalBuilder> threadLocalBuilder;
    private final NodeMappingBuilder.Capturing nodeMappingBuilder;

    private final NodeImporter nodeImporter;

    private final Lock lock;

    private final IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyToken;
    private final boolean hasProperties;

    NodesBuilder(
        long maxOriginalId,
        long nodeCount,
        int concurrency,
        ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping,
        Map<NodeLabel, HugeAtomicBitSet> nodeLabelBitSetMap,
        IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping,
        IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyKey,
        NodeMappingBuilder.Capturing nodeMappingBuilder,
        InternalIdMappingBuilder<? extends IdMappingAllocator> internalIdMappingBuilder,
        boolean hasLabelInformation,
        boolean hasProperties,
        AllocationTracker tracker
    ) {
        this.maxOriginalId = maxOriginalId;
        this.nodeCount = nodeCount;
        this.concurrency = concurrency;
        this.elementIdentifierLabelTokenMapping = elementIdentifierLabelTokenMapping;
        this.nodeLabelBitSetMap = nodeLabelBitSetMap;
        this.labelTokenNodeLabelMapping = labelTokenNodeLabelMapping;
        this.tracker = tracker;
        this.nextLabelId = 0;
        this.lock = new ReentrantLock(true);
        this.buildersByLabelTokenAndPropertyToken = buildersByLabelTokenAndPropertyKey;
        this.hasProperties = hasProperties;

        this.nodeMappingBuilder = nodeMappingBuilder;

        this.nodeImporter = new NodeImporter(
            internalIdMappingBuilder,
            nodeLabelBitSetMap,
            labelTokenNodeLabelMapping,
            hasProperties,
            tracker
        );

        var seenIds = HugeAtomicBitSet.create(maxOriginalId + 1, tracker);

        Function<NodeLabel, Integer> labelTokenIdFn = elementIdentifierLabelTokenMapping.isEmpty()
            ? this::getOrCreateLabelTokenId
            : this::getLabelTokenId;
        BiFunction<Integer, String, NodePropertiesFromStoreBuilder> propertyBuilderFn = buildersByLabelTokenAndPropertyKey.isEmpty()
            ? this::getOrCreatePropertyBuilder
            : this::getPropertyBuilder;
        LongPredicate seenNodeIdPredicate = maxOriginalId == UNKNOWN_MAX_ID
            ? nodeId -> false
            : seenIds::getAndSet;
        this.threadLocalBuilder = AutoCloseableThreadLocal.withInitial(
            () -> new NodesBuilder.ThreadLocalBuilder(
                nodeImporter,
                seenNodeIdPredicate,
                hasLabelInformation,
                hasProperties,
                labelTokenIdFn,
                propertyBuilderFn,
                buildersByLabelTokenAndPropertyKey
            )
        );
    }

    public void addNode(long originalId, NodeLabel... nodeLabels) {
        this.threadLocalBuilder.get().addNode(originalId, nodeLabels);
    }

    public void addNode(long originalId, Map<String, Value> properties, NodeLabel... nodeLabels) {
        this.threadLocalBuilder.get().addNode(originalId, properties, nodeLabels);
    }

    public NodeMappingAndProperties build() {
        return build(maxOriginalId, false);
    }

    public NodeMappingAndProperties buildChecked(long highestNeoId) {
        return build(highestNeoId, true);
    }

    public NodeMappingAndProperties build(long highestNeoId, boolean checkDuplicateIds) {
        this.threadLocalBuilder.close();

        var nodeMapping = this.nodeMappingBuilder.build(
            nodeLabelBitSetMap,
            highestNeoId,
            concurrency,
            checkDuplicateIds,
            tracker
        );

        Optional<Map<NodeLabel, Map<String, NodeProperties>>> nodeProperties = Optional.empty();
        if (hasProperties) {
            nodeProperties = Optional.of(buildProperties());
        }
        return ImmutableNodeMappingAndProperties.of(nodeMapping, nodeProperties);
    }

    private Map<NodeLabel, Map<String, NodeProperties>> buildProperties() {
        Map<NodeLabel, Map<String, NodeProperties>> nodePropertiesByLabel = new HashMap<>();
        for (IntObjectCursor<Map<String, NodePropertiesFromStoreBuilder>> propertyBuilderByLabelToken : this.buildersByLabelTokenAndPropertyToken) {
            var nodeLabels = labelTokenNodeLabelMapping.get(propertyBuilderByLabelToken.key);
            nodeLabels.forEach(nodeLabel ->
                propertyBuilderByLabelToken.value.forEach((propertyKey, propertyBuilder) -> nodePropertiesByLabel
                    .computeIfAbsent(nodeLabel, __ -> new HashMap<>()).put(propertyKey, propertyBuilder.build())
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
            propertyBuildersByPropertyKey.put(propertyKey, NodePropertiesFromStoreBuilder.of(nodeCount, tracker, NO_PROPERTY_VALUE));
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
    public interface NodeMappingAndProperties {
        NodeMapping nodeMapping();

        Optional<Map<NodeLabel, Map<String, NodeProperties>>> nodeProperties();

        default Optional<Map<String, NodeProperties>> unionNodeProperties() {
            if (nodeProperties().isEmpty()) {
                return Optional.empty();
            }

            Map<String, Map<NodeLabel, NodeProperties>> nodePropertiesByKeyAndLabel = new HashMap<>();
            nodeProperties().get().forEach((nodeLabel, propertiesByKey) -> {
                propertiesByKey.forEach((propertyKey, propertyValues) -> {
                    nodePropertiesByKeyAndLabel
                        .computeIfAbsent(propertyKey, __ -> new HashMap<>())
                        .put(nodeLabel, propertyValues);
                });
            });

            Map<String, NodeProperties> unionNodeProperties = nodePropertiesByKeyAndLabel
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new UnionNodeProperties(nodeMapping(), entry.getValue())
                ));
            return Optional.of(unionNodeProperties);
        }

        default Map<String, NodeProperties> unionNodePropertiesOrThrow() {
            return unionNodeProperties()
                .orElseThrow(() -> new IllegalArgumentException("Expected node properties to be present"));
        }
    }

    private static class ThreadLocalBuilder implements AutoCloseable {

        private static final long[] ANY_LABEL_ARRAY = { ANY_LABEL };

        private final LongPredicate seenNodeIdPredicate;
        private final NodesBatchBuffer buffer;
        private final Function<NodeLabel, Integer> labelTokenIdFn;
        private final BiFunction<Integer, String, NodePropertiesFromStoreBuilder> propertyBuilderFn;
        private final NodeImporter nodeImporter;
        private final IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyKey;
        private final List<Map<String, Value>> batchNodeProperties;

        ThreadLocalBuilder(
            NodeImporter nodeImporter,
            LongPredicate seenNodeIdPredicate,
            boolean hasLabelInformation,
            boolean hasProperties,
            Function<NodeLabel, Integer> labelTokenIdFn,
            BiFunction<Integer, String, NodePropertiesFromStoreBuilder> propertyBuilderFn,
            IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyKey
        ) {
            this.seenNodeIdPredicate = seenNodeIdPredicate;
            this.labelTokenIdFn = labelTokenIdFn;
            this.propertyBuilderFn = propertyBuilderFn;

            this.buffer = new NodesBatchBufferBuilder()
                .capacity(SparseLongArray.toValidBatchSize(ParallelUtil.DEFAULT_BATCH_SIZE))
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

                buffer.add(originalId, NO_SUCH_PROPERTY_KEY, labels);
                if (buffer.isFull()) {
                    flushBuffer();
                    reset();
                }
            }
        }

        public void addNode(long originalId, Map<String, Value> properties, NodeLabel... nodeLabels) {
            if (!seenNodeIdPredicate.test(originalId)) {
                long[] labels = labelTokens(nodeLabels);

                int propertyReference = batchNodeProperties.size();
                batchNodeProperties.add(properties);

                buffer.add(originalId, propertyReference, labels);
                if (buffer.isFull()) {
                    flushBuffer();
                    reset();
                }
            }
        }

        private long[] labelTokens(NodeLabel... nodeLabels) {
            if (nodeLabels == null || nodeLabels.length == 0) {
                return ANY_LABEL_ARRAY;
            }
            long[] labelIds = new long[nodeLabels.length];

            for (int i = 0; i < nodeLabels.length; i++) {
                labelIds[i] = labelTokenIdFn.apply(nodeLabels[i]);
            }

            return labelIds;
        }

        private void flushBuffer() {
            this.nodeImporter.importNodes(buffer, (nodeReference, labelIds, propertiesReference, internalId) -> {
                if (propertiesReference != NO_SUCH_PROPERTY_KEY) {
                    Map<String, Value> properties = batchNodeProperties.get((int) propertiesReference);
                    MutableInt importedProperties = new MutableInt(0);
                    properties.forEach((propertyKey, propertyValue) -> importedProperties.add(importProperty(
                        internalId,
                        labelIds,
                        propertyKey,
                        propertyValue
                    )));
                    return importedProperties.intValue();
                }
                return 0;
            });
        }

        private void reset() {
            buffer.reset();
            batchNodeProperties.clear();
        }

        @Override
        public void close() {
            flushBuffer();
        }

        private int importProperty(long nodeId, long[] labels, String propertyKey, Value value) {
            int propertiesImported = 0;

            for (long label : labels) {
                if (label == IGNORE || label == ANY_LABEL) {
                    continue;
                }

                var nodePropertyBuilder = propertyBuilderFn.apply((int) label, propertyKey);
                if (nodePropertyBuilder != null) {
                    nodePropertyBuilder.set(nodeId, value);
                    propertiesImported++;
                }
            }

            if (buildersByLabelTokenAndPropertyKey.containsKey(ANY_LABEL)) {
                var nodePropertiesBuilder = buildersByLabelTokenAndPropertyKey
                    .get(ANY_LABEL)
                    .get(propertyKey);
                if (nodePropertiesBuilder != null) {
                    nodePropertiesBuilder.set(nodeId, value);
                    propertiesImported++;
                }
            }

            return propertiesImported;
        }
    }
}
