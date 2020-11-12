/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.LokiInternalIdMappingBuilder;
import org.neo4j.graphalgo.core.loading.NodeImporter;
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

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public class NodesBuilder {

    private final long maxOriginalId;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final SparseLongArray.Builder sparseLongArrayBuilder;

    private int nextLabelId;
    private final Map<NodeLabel, Integer> elementIdentifierLabelTokenMapping;
    private final Map<NodeLabel, HugeAtomicBitSet> nodeLabelBitSetMap;
    private final IntObjectHashMap<List<NodeLabel>> labelTokenNodeLabelMapping;

    private final AutoCloseableThreadLocal<ThreadLocalBuilder> threadLocalBuilder;
    private final LokiInternalIdMappingBuilder hugeInternalIdMappingBuilder;
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
        this.elementIdentifierLabelTokenMapping = new ConcurrentHashMap<>();
        this.nodeLabelBitSetMap = new ConcurrentHashMap<>();
        this.labelTokenNodeLabelMapping = new IntObjectHashMap<>();
        this.lock = new ReentrantLock(true);

        // TODO: why is this maxOriginalId + 1, couldn't it be just nodeCount?
        this.hugeInternalIdMappingBuilder = LokiInternalIdMappingBuilder.of(maxOriginalId + 1, tracker);
        this.sparseLongArrayBuilder = SparseLongArray.builder(maxOriginalId + 1);
        this.nodeImporter = new NodeImporter(
            hugeInternalIdMappingBuilder,
            nodeLabelBitSetMap,
            labelTokenNodeLabelMapping,
            tracker,
            sparseLongArrayBuilder
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

    public IdMap build() {
        this.threadLocalBuilder.close();

        return org.neo4j.graphalgo.core.loading.IdMapBuilder.build(
            hugeInternalIdMappingBuilder,
            sparseLongArrayBuilder,
            nodeLabelBitSetMap,
            maxOriginalId,
            concurrency,
            tracker
        );
    }

    private int labelTokenId(NodeLabel nodeLabel) {
        var token = elementIdentifierLabelTokenMapping.get(nodeLabel);
        if (token == null) {
            lock.lock();
            token = elementIdentifierLabelTokenMapping.get(nodeLabel);
            if (token == null) {
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
