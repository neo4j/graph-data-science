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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation.Labels;

public final class LabelPropagationTests {

    public static LongObjectMap<LongArrayList> groupByPartition(Labels labels) {
        if (labels == null) {
            return null;
        }
        LongObjectMap<LongArrayList> cluster = new LongObjectHashMap<>();
        for (long node = 0L, l = labels.size(); node < l; node++) {
            long key = labels.labelFor(node);
            LongArrayList ids = cluster.get(key);
            if (ids == null) {
                ids = new LongArrayList();
                cluster.put(key, ids);
            }
            ids.add(node);
        }

        return cluster;
    }

    public static IntObjectMap<IntArrayList> groupByPartitionInt(Labels labels) {
        if (labels == null) {
            return null;
        }
        IntObjectMap<IntArrayList> cluster = new IntObjectHashMap<>();
        for (int node = 0, l = Math.toIntExact(labels.size()); node < l; node++) {
            int key = Math.toIntExact(labels.labelFor(node));
            IntArrayList ids = cluster.get(key);
            if (ids == null) {
                ids = new IntArrayList();
                cluster.put(key, ids);
            }
            ids.add(node);
        }

        return cluster;
    }

    private LabelPropagationTests() {
        throw new UnsupportedOperationException("No instances");
    }
}
