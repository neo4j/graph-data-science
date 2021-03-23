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
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.ObjectIntScatterMap;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.NodeMapping;
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
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;
import org.neo4j.graphalgo.utils.AutoCloseableThreadLocal;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.neo4j.graphalgo.core.GraphDimensions.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public class NodesBuilder {

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

    NodesBuilder(
        long maxOriginalId,
        boolean hasLabelInformation,
        int concurrency,
        AllocationTracker tracker
    ) {
        this.maxOriginalId = maxOriginalId;
        this.concurrency = concurrency;
        this.tracker = tracker;
        this.nextLabelId = 0;
        this.elementIdentifierLabelTokenMapping = new ObjectIntScatterMap<>();
        this.nodeLabelBitSetMap = new ConcurrentHashMap<>();
        this.labelTokenNodeLabelMapping = new IntObjectHashMap<>();
        this.lock = new ReentrantLock(true);

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
            false,
            tracker
        );

        var seenIds = HugeAtomicBitSet.create(maxOriginalId + 1, tracker);

        this.threadLocalBuilder = AutoCloseableThreadLocal.withInitial(
            () -> new ThreadLocalBuilder(
                nodeImporter,
                seenIds,
                hasLabelInformation,
                this::labelTokenId
            )
        );
    }

    public void addNode(long originalId, NodeLabel... nodeLabels) {
        this.threadLocalBuilder.get().addNode(originalId, nodeLabels);
    }

    public NodeMapping build() {
        this.threadLocalBuilder.close();

        var graphDimensions = ImmutableGraphDimensions.builder()
            .nodeCount(maxOriginalId)
            .highestNeoId(maxOriginalId)
            .build();

        return this.nodeMappingBuilder.build(
            nodeLabelBitSetMap,
            graphDimensions,
            concurrency,
            tracker
        );
    }

    private int labelTokenId(NodeLabel nodeLabel) {
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

    private static class ThreadLocalBuilder implements AutoCloseable {

        private final HugeAtomicBitSet seenIds;
        private final NodesBatchBuffer buffer;
        private final Function<NodeLabel, Integer> labelTokenIdFn;
        private final NodeImporter nodeImporter;

        ThreadLocalBuilder(
            NodeImporter nodeImporter,
            HugeAtomicBitSet seenIds,
            boolean hasLabelInformation,
            Function<NodeLabel, Integer> labelTokenIdFn
        ) {
            this.seenIds = seenIds;
            this.labelTokenIdFn = labelTokenIdFn;

            this.buffer = new NodesBatchBufferBuilder()
                .capacity(SparseLongArray.toValidBatchSize(ParallelUtil.DEFAULT_BATCH_SIZE))
                .hasLabelInformation(hasLabelInformation)
                .readProperty(false)
                .build();
            this.nodeImporter = nodeImporter;
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

        private long[] labelTokens(NodeLabel... nodeLabels) {
            long[] labelIds = new long[nodeLabels.length];

            for (int i = 0; i < nodeLabels.length; i++) {
                labelIds[i] = labelTokenIdFn.apply(nodeLabels[i]);
            }

            return labelIds;
        }

        private void flushBuffer() {
            this.nodeImporter.importNodes(buffer, (nodeReference, labelIds, propertiesReference, internalId) -> 0);
        }

        private void reset() {
            buffer.reset();
        }

        @Override
        public void close() {
            flushBuffer();
        }
    }
}
