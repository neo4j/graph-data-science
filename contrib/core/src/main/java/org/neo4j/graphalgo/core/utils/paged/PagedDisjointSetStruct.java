/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.utils.paged;


import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.graphalgo.api.HugeIdMapping;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class PagedDisjointSetStruct {

    private final HugeLongArray parent;
    private final HugeLongArray depth;
    private final long capacity;

    public PagedDisjointSetStruct(long capacity, AllocationTracker tracker) {
        parent = HugeLongArray.newArray(capacity, tracker);
        depth = HugeLongArray.newArray(capacity, tracker);
        this.capacity = capacity;
    }

    public PagedDisjointSetStruct reset() {
        parent.fill(-1);
        return this;
    }

    public long capacity() {
        return capacity;
    }

    public boolean connected(long p, long q) {
        return find(p) == find(q);
    }

    public long find(long p) {
        return findPC(p);
    }

    private long findPC(long p) {
        long pv = parent.get(p);
        if (pv == -1L) {
            return p;
        }
        // path compression optimization
        // TODO
        long value = find(pv);
        parent.set(p, value);
        return value;
    }

    public void union(long p, long q) {
        final long pSet = find(p);
        final long qSet = find(q);
        if (pSet == qSet) {
            return;
        }
        // weighted union rule optimization
        long dq = depth.get(qSet);
        long dp = depth.get(pSet);
        if (dp < dq) {
            // attach the smaller tree to the root of the bigger tree
            parent.set(pSet, qSet);
        } else if (dp > dq) {
            parent.set(qSet, pSet);
        } else {
            parent.set(qSet, pSet);
            depth.addTo(pSet, depth.get(qSet) + 1);
        }
    }

    public PagedDisjointSetStruct merge(PagedDisjointSetStruct other) {

        if (other.capacity != this.capacity) {
            throw new IllegalArgumentException("Different Capacity");
        }

        HugeLongArray.Cursor others = other.parent.cursor(other.parent.newCursor());
        long i = 0L;
        while (others.next()) {
            long[] array = others.array;
            int offset = others.offset;
            int limit = others.limit;
            while (offset < limit) {
                if (array[offset++] != -1L) {
                    union(i, other.find(i));
                }
                ++i;
            }
        }

        return this;
    }

    public long findNoOpt(final long nodeId) {
        long p = nodeId;
        long np;
        while ((np = parent.get(p)) != -1) {
            p = np;
        }
        return p;
    }

    public int getSetCount() {
        LongScatterSet set = new LongScatterSet();
        for (long i = 0L; i < capacity; ++i) {
            long setId = find(i);
            set.add(setId);
        }
        return set.size();
    }

    public Stream<DisjointSetStruct.Result> resultStream(HugeIdMapping idMapping) {

        return LongStream.range(HugeIdMapping.START_NODE_ID, idMapping.nodeCount())
                .mapToObj(mappedId ->
                        new DisjointSetStruct.Result(
                                idMapping.toOriginalNodeId(mappedId),
                                find(mappedId)));
    }

    public static final class Translator implements PropertyTranslator.OfLong<PagedDisjointSetStruct> {

        public static final PropertyTranslator<PagedDisjointSetStruct> INSTANCE = new Translator();

        @Override
        public long toLong(final PagedDisjointSetStruct data, final long nodeId) {
            return data.findNoOpt(nodeId);
        }
    }
}
