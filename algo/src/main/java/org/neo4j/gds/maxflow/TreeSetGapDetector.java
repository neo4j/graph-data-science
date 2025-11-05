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
package org.neo4j.gds.maxflow;

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;

import java.util.TreeSet;

public class TreeSetGapDetector implements GapDetector {
    private HugeObjectArray<TreeSet> gapMap;
    private final HugeLongArray label;
    private final long nodeCount;

    public TreeSetGapDetector(HugeLongArray label, long nodeCount) {
        this.label = label;
        this.nodeCount = nodeCount;
        this.gapMap =HugeObjectArray.newArray(TreeSet.class, nodeCount+2);
    }

    @Override
    public void resetCounts() {
        gapMap.setAll(_v -> new TreeSet<Long>());
        for (long v = 0; v < nodeCount; v++) {
            gapMap.get(label.get(v)).add(v);
        }
    }

    @Override
    public void relabel(long gap) {
        for(long d = gap+1; d < nodeCount; d++) {
            gapMap.get(d).forEach(v -> {
                label.set((long)v, nodeCount);
            });
            gapMap.get(d).clear();
        }
    }

    @Override
    public boolean moveFrom(long node, long from, long to) {
        gapMap.get(from).remove(node);
        gapMap.get(to).add(node);
        return  isEmpty(from);
    }

     boolean isEmpty(long gap) {
        return gapMap.get(gap).isEmpty();
    }
}
