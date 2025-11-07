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

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;

public class ArrayListGapDetector implements GapDetector {
    private final HugeIntArray position;
    private final HugeLongArray label;
    private final long nodeCount;
    private final HugeObjectArray<LongArrayList> gapMap;

    public ArrayListGapDetector(HugeLongArray label, long nodeCount) {
        this.label = label;
        this.nodeCount = nodeCount;
        this.position = HugeIntArray.newArray(nodeCount);
        this.gapMap = HugeObjectArray.newArray(LongArrayList.class, nodeCount + 2);
    }

    @Override
    public void resetCounts() {
        gapMap.setAll(_v -> new LongArrayList(10));
        for (long v = 0; v < nodeCount; v++) {
            place(v, label.get(v));
        }
    }

    @Override
    public void relabel(long gap) {
        for (long d = gap + 1; d < nodeCount; d++) {
            var gapElements = gapMap.get(d);
            for (int i = 0; i < gapElements.size(); ++i) {
                var entry = gapElements.get(i);
                label.set(entry, nodeCount);
            }
            gapElements.clear();
            gapElements.resize(10);
        }
    }

    private void place(long v, long to) {
        var gapPlace = gapMap.get(to);
        position.set(v, gapPlace.size());
        gapPlace.add(v);
    }

    private void remove(long v, long from) {
        var gapPlace = gapMap.get(from);
        var vPosition = position.get(v);
        if (vPosition != gapPlace.size() - 1) {
            var last = gapPlace.get(gapPlace.size() - 1);
            gapPlace.set(vPosition, last);
            position.set(last, vPosition);
        }
        gapPlace.elementsCount--;
    }

    @Override
    public boolean moveFrom(long node, long from, long to) {
        remove(node, from);
        place(node, to);
        return isEmpty(from);
    }

    boolean isEmpty(long gap) {
        return gapMap.get(gap).isEmpty();
    }
}
