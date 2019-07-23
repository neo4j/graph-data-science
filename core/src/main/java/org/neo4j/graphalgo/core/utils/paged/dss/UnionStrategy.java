/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.paged.dss;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

public interface UnionStrategy {

    void union(long p, long q, DisjointSetStruct dss);

    /**
     * Picks the minimum set id as representative when merging two sets.
     */
    final class ByMin implements UnionStrategy {

        public static final String NAME = "min";

        @Override
        public void union(final long p, final long q, final DisjointSetStruct dss) {
            long pRoot = dss.findAndBalance(p);
            long qRoot = dss.findAndBalance(q);

            long pSet = dss.setIdOfRoot(pRoot);
            long qSet = dss.setIdOfRoot(qRoot);

            if (pSet < qSet) {
                dss.parent().set(qRoot, pRoot);
            } else if (qSet < pSet) {
                dss.parent().set(pRoot, qRoot);
            }
        }
    }

    /**
     * Uses <a href="https://en.wikipedia.org/wiki/Disjoint-set_data_structure#by_rank%22&gt;Rank&lt;/a&gt;">rank based tree balancing.</a>
     */
    final class ByRank implements UnionStrategy {

        public static final String NAME = "rank";

        private final HugeLongArray depth;

        public ByRank(long capacity, AllocationTracker tracker) {
            depth = HugeLongArray.newArray(capacity, tracker);
        }

        @Override
        public void union(final long p, final long q, final DisjointSetStruct dss) {
            final long pSet = dss.findAndBalance(p);
            final long qSet = dss.findAndBalance(q);
            if (pSet == qSet) {
                return;
            }
            // weighted union rule optimization
            long dq = depth.get(qSet);
            long dp = depth.get(pSet);
            if (dp < dq) {
                // attach the smaller tree to the root of the bigger tree
                dss.parent().set(pSet, qSet);
            } else if (dp > dq) {
                dss.parent().set(qSet, pSet);
            } else {
                dss.parent().set(qSet, pSet);
                depth.addTo(pSet, dq + 1);
            }
        }
    }
}
