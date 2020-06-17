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
package org.neo4j.gds.core.pagecached;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.LongPredicate;

public class IdMap implements NodeMapping, NodeIterator, BatchNodeIterable, AutoCloseable {

    private static final Set<NodeLabel> ALL_NODES_LABELS = Set.of(NodeLabel.ALL_NODES);

    private final long nodeCount;

    private final Map<NodeLabel, BitSet> labelInformation;

    private final PagedFile graphIds;
    private final ReverseIdMapping nodeToGraphIds;
    private final PageCursor graphIdsCursor;

    /**
     * initialize the map with pre-built sub arrays
     */
    IdMap(
        PagedFile graphIds,
        ReverseIdMapping nodeToGraphIds,
        Map<NodeLabel, BitSet> labelInformation,
        long nodeCount
    ) throws IOException {
        this.graphIds = graphIds;
        this.nodeToGraphIds = nodeToGraphIds;
        this.labelInformation = labelInformation;
        this.nodeCount = nodeCount;
        this.graphIdsCursor = Neo4jProxy.pageFileIO(graphIds, 0, PagedFile.PF_SHARED_READ_LOCK, PageCursorTracer.NULL);
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        try {
            return nodeToGraphIds.get(nodeId);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return getFromPage(nodeId, graphIdsCursor);
    }

    private long getFromPage(long nodeId, PageCursor pageCursor) {
        var pageIndex = nodeId / PageCache.PAGE_SIZE;
        try {
            if (pageCursor.next(pageIndex)) {
                var indexInPage = nodeId % PageCache.PAGE_SIZE;
                return pageCursor.getLong((int) (indexInPage * Long.BYTES));
            }
            return -1L;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean contains(final long nodeId) {
        return toMappedNodeId(nodeId) != -1L;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        final long count = nodeCount();
        for (long i = 0L; i < count; i++) {
            if (!consumer.test(i)) {
                return;
            }
        }
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return new IdIterator(nodeCount());
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        return LazyBatchCollection.of(
                nodeCount(),
                batchSize,
                IdIterable::new);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return labelInformation.isEmpty()
            ? ALL_NODES_LABELS
            : labelInformation.keySet();
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        if (labelInformation.isEmpty()) {
            return ALL_NODES_LABELS;
        } else {
            Set<NodeLabel> set = new HashSet<>();
            for (var labelAndBitSet : labelInformation.entrySet()) {
                if (labelAndBitSet.getValue().get(nodeId)) {
                    set.add(labelAndBitSet.getKey());
                }
            }
            return set;
        }
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        BitSet bitSet = labelInformation.get(label);
        return bitSet != null && bitSet.get(nodeId);
    }

    @Override
    public void close() throws IOException {
        this.graphIdsCursor.close();
        this.graphIds.close();
        this.nodeToGraphIds.close();
    }

    public static final class IdIterable implements PrimitiveLongIterable {
        private final long start;
        private final long length;

        public IdIterable(long start, long length) {
            this.start = start;
            this.length = length;
        }

        @Override
        public PrimitiveLongIterator iterator() {
            return new IdIterator(start, length);
        }
    }

    public static final class IdIterator implements PrimitiveLongIterator {

        private long current;
        private long limit; // exclusive upper bound

        public IdIterator(long length) {
            this.current = 0;
            this.limit = length;
        }

        private IdIterator(long start, long length) {
            this.current = start;
            this.limit = start + length;
        }

        @Override
        public boolean hasNext() {
            return current < limit;
        }

        @Override
        public long next() {
            return current++;
        }
    }
}
