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
package org.neo4j.gds;

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.api.Graph;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public final class GraphHelper {

    public static void assertRelationships(Graph graph, long node, long... expected) {
        LongArrayList idList = new LongArrayList();
        graph.forEachRelationship(node, (s, t) -> {
            idList.add(t);
            return true;
        });
        long[] ids = idList.toArray();
        Arrays.sort(ids);
        Arrays.sort(expected);
        assertArrayEquals(expected, ids);
    }

    private GraphHelper() {
        throw new UnsupportedOperationException("No instances");
    }
}
