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
import com.carrotsearch.hppc.ObjectIntScatterMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.UnionNodeProperties;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.loading.IdMapImplementations;
import org.neo4j.graphalgo.core.loading.IdMappingAllocator;
import org.neo4j.graphalgo.core.loading.InternalHugeIdMappingBuilder;
import org.neo4j.graphalgo.core.loading.InternalIdMappingBuilder;
import org.neo4j.graphalgo.core.loading.InternalSequentialBitIdMappingBuilder;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;
import static org.neo4j.graphalgo.core.GraphDimensions.IGNORE;
import static org.neo4j.graphalgo.core.GraphDimensions.NO_SUCH_LABEL;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public final class NodesBuilder {

    private final long maxOriginalId;
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

    static NodesBuilder withoutSchema(
        long maxOriginalId,
        boolean hasLabelInformation,
        boolean hasProperties,
        int concurrency,
        AllocationTracker tracker
    ) {
        return new NodesBuilder(
            maxOriginalId,
            concurrency,
            new ObjectIntScatterMap<>(),
            new ConcurrentHashMap<>(),
            new IntObjectHashMap<>(),
            null,
            hasLabelInformation,
            hasProperties,
            tracker
        );
    }

    static NodesBuilder fromSchema(
        long maxOriginalId,
        long nodeCount,
        int concurrency,
        NodeSchema nodeSchema,
        AllocationTracker tracker
    ) {
        var nodeLabels = nodeSchema.availableLabels();

        var elementIdentifierLabelTokenMapping = new ObjectIntScatterMap<NodeLabel>();
        var labelTokenNodeLabelMapping = new IntObjectHashMap<List<NodeLabel>>();
        IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> builderByLabelTokenAndPropertyToken = new IntObjectHashMap<>();

        MutableInt labelTokenCounter = new MutableInt(0);
        nodeLabels.forEach(nodeLabel -> {
            int labelToken = nodeLabel == NodeLabel.ALL_NODES
                ? ANY_LABEL
                : labelTokenCounter.getAndIncrement();

            elementIdentifierLabelTokenMapping.put(nodeLabel, labelToken);
            labelTokenNodeLabelMapping.put(labelToken, List.of(nodeLabel));
            builderByLabelTokenAndPropertyToken.put(labelToken, new HashMap<>());
            nodeSchema.properties().get(nodeLabel).forEach((propertyKey, propertySchema) -> {
                builderByLabelTokenAndPropertyToken.get(labelToken).put(
                    propertyKey,
                    NodePropertiesFromStoreBuilder.of(nodeCount, tracker, propertySchema.defaultValue())
                );
            });
        });

        boolean hasLabelInformation = !(nodeLabels.isEmpty() || (nodeLabels.size() == 1 && nodeLabels.contains(NodeLabel.ALL_NODES)));
        return new NodesBuilder(
            maxOriginalId,
            concurrency,
            elementIdentifierLabelTokenMapping,
            new ConcurrentHashMap<>(nodeLabels.size()),
            labelTokenNodeLabelMapping,
            builderByLabelTokenAndPropertyToken,
            hasLabelInformation,
            nodeSchema.hasProperties(),
            tracker
        );
    }

    private NodesBuilder(
        long maxOriginalId,
        int concurrency,
        ObjectIntMap<NodeLabel> elementIdentifierLabelTokenMapping,
        Map<NodeLabel, HugeAtomicBitSet> nodeLabelBitSetMap,
        IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping,
        IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyKey,
        boolean hasLabelInformation,
        boolean hasProperties,
        AllocationTracker tracker
    ) {
        this.maxOriginalId = maxOriginalId;
        this.concurrency = concurrency;
        this.elementIdentifierLabelTokenMapping = elementIdentifierLabelTokenMapping;
        this.nodeLabelBitSetMap = nodeLabelBitSetMap;
        this.labelTokenNodeLabelMapping = labelTokenNodeLabelMapping;
        this.tracker = tracker;
        this.nextLabelId = 0;
        this.lock = new ReentrantLock(true);
        this.buildersByLabelTokenAndPropertyToken = buildersByLabelTokenAndPropertyKey;
        this.hasProperties = hasProperties;

        // this is maxOriginalId + 1, because it is the capacity for the neo -> gds mapping, where we need to
        // be able to include the highest possible id
        InternalIdMappingBuilder<? extends IdMappingAllocator> internalIdMappingBuilder;
        // The sequential bitidmap builder does not support labels that are added *during* the import.
        // The default bitidmap builder does requires nodes being added in specific batches (super blocks), so it cannot be used here.
        if (IdMapImplementations.useBitIdMap() && !hasLabelInformation) {
            var idMappingBuilder = InternalSequentialBitIdMappingBuilder.of(maxOriginalId + 1, tracker);
            this.nodeMappingBuilder = IdMapImplementations.sequentialBitIdMapBuilder(idMappingBuilder);
            internalIdMappingBuilder = idMappingBuilder;
        } else {
            var idMappingBuilder = InternalHugeIdMappingBuilder.of(maxOriginalId + 1, tracker);
            this.nodeMappingBuilder = IdMapImplementations.hugeIdMapBuilder(idMappingBuilder);
            internalIdMappingBuilder = idMappingBuilder;
        }

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
        this.threadLocalBuilder = AutoCloseableThreadLocal.withInitial(
            () -> new NodesBuilder.ThreadLocalBuilder(
                nodeImporter,
                seenIds,
                hasLabelInformation,
                hasProperties,
                labelTokenIdFn,
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
        this.threadLocalBuilder.close();

        var graphDimensions = ImmutableGraphDimensions.builder()
            .nodeCount(maxOriginalId)
            .highestNeoId(maxOriginalId)
            .build();

        var nodeMapping = this.nodeMappingBuilder.build(
            nodeLabelBitSetMap,
            graphDimensions,
            concurrency,
            tracker
        );

        Optional<Map<String, NodeProperties>> nodeProperties = Optional.empty();
        if (hasProperties) {
            nodeProperties = Optional.of(buildProperties(nodeMapping));
        }
        return ImmutableNodeMappingAndProperties.of(nodeMapping, nodeProperties);
    }

    private Map<String, NodeProperties> buildProperties(NodeMapping nodeMapping) {
        this.threadLocalBuilder.close();

        Map<String, Map<NodeLabel, NodeProperties>> nodePropertiesByKeyAndLabel = new HashMap<>();
        for (IntObjectCursor<Map<String, NodePropertiesFromStoreBuilder>> propertyBuilderByLabelToken : this.buildersByLabelTokenAndPropertyToken) {
            var nodeLabels = labelTokenNodeLabelMapping.get(propertyBuilderByLabelToken.key);
            nodeLabels.forEach(nodeLabel ->
                propertyBuilderByLabelToken.value.forEach((propertyKey, propertyBuilder) -> nodePropertiesByKeyAndLabel
                    .computeIfAbsent(propertyKey, __ -> new HashMap<>()).put(nodeLabel, propertyBuilder.build())
                )
            );
        }

        return nodePropertiesByKeyAndLabel.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> new UnionNodeProperties(nodeMapping, entry.getValue())
        ));
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

    @ValueClass
    public interface NodeMappingAndProperties {
        NodeMapping nodeMapping();
        Optional<Map<String, NodeProperties>> nodeProperties();

        default Map<String, NodeProperties> nodePropertiesOrThrow() {
            return nodeProperties()
                .orElseThrow(() -> new IllegalArgumentException("Expected node properties to be present"));
        }
    }

    private static class ThreadLocalBuilder implements AutoCloseable {

        private final HugeAtomicBitSet seenIds;
        private final NodesBatchBuffer buffer;
        private final Function<NodeLabel, Integer> labelTokenIdFn;
        private final NodeImporter nodeImporter;
        private final IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyKey;
        private final List<Map<String, Value>> batchNodeProperties;

        ThreadLocalBuilder(
            NodeImporter nodeImporter,
            HugeAtomicBitSet seenIds,
            boolean hasLabelInformation,
            boolean hasProperties,
            Function<NodeLabel, Integer> labelTokenIdFn,
            IntObjectMap<Map<String, NodePropertiesFromStoreBuilder>> buildersByLabelTokenAndPropertyKey
        ) {
            this.seenIds = seenIds;
            this.labelTokenIdFn = labelTokenIdFn;

            this.buffer = new NodesBatchBufferBuilder()
                .capacity(SparseLongArray.toValidBatchSize(ParallelUtil.DEFAULT_BATCH_SIZE))
                .hasLabelInformation(hasLabelInformation)
                .readProperty(hasProperties)
                .build();
            this.nodeImporter = nodeImporter;
            this.buildersByLabelTokenAndPropertyKey = buildersByLabelTokenAndPropertyKey;
            this.batchNodeProperties = new ArrayList<>();
        }

        public void addNode(long originalId, NodeLabel... nodeLabels) {
            if (!seenIds.getAndSet(originalId)) {
                long[] labels = labelTokens(nodeLabels);

                buffer.add(originalId, NO_SUCH_PROPERTY_KEY, labels);
                if (buffer.isFull()) {
                    flushBuffer();
                    reset();
                }
            }
        }

        public void addNode(long originalId, Map<String, Value> properties, NodeLabel... nodeLabels) {
            if (!seenIds.getAndSet(originalId)) {
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
                return new long[]{ ANY_LABEL };
            }
            long[] labelIds = new long[nodeLabels.length];

            for (int i = 0; i < nodeLabels.length; i++) {
                labelIds[i] = labelTokenIdFn.apply(nodeLabels[i]);
            }

            return labelIds;
        }

        private void flushBuffer() {
            this.nodeImporter.importNodes(buffer, (nodeReference, labelIds, propertiesReference, internalId) -> {
                Map<String, Value> properties = batchNodeProperties.get((int) propertiesReference);
                return properties
                    .entrySet()
                    .stream()
                    .mapToInt(entry -> importProperty(internalId, labelIds, entry.getKey(), entry.getValue()))
                    .sum();
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

                Map<String, NodePropertiesFromStoreBuilder> buildersByPropertyId = buildersByLabelTokenAndPropertyKey.get((int) label);
                if (buildersByPropertyId != null) {
                    NodePropertiesFromStoreBuilder nodePropertiesBuilder = buildersByPropertyId.get(propertyKey);
                    if (nodePropertiesBuilder != null) {
                        nodePropertiesBuilder.set(nodeId, value);
                        propertiesImported++;
                    }
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
