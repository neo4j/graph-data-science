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

package org.neo4j.graphalgo;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.sorting.IndirectSort;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AscendingLongComparator;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public final class GraphHelper {

    public static long[] collectTargetIds(final Graph graph, long sourceId) {
        LongStream.Builder outIds = LongStream.builder();
        graph.forEachRelationship(graph.toMappedNodeId(sourceId), Direction.OUTGOING,
                (sourceNodeId, targetNodeId) -> {
                    outIds.add(targetNodeId);
                    return true;
                });
        return outIds.build().sorted().toArray();
    }

    public static double[] collectTargetProperties(final Graph graph, long sourceId) {
        DoubleStream.Builder outWeights = DoubleStream.builder();
        graph.forEachRelationship(graph.toMappedNodeId(sourceId), Direction.OUTGOING,
                0D, (sourceNodeId, targetNodeId, weight) -> {
                    outWeights.add(weight);
                    return true;
                });
        return outWeights.build().toArray();
    }

    public static void assertOutProperties(Graph graph, long node, double... expected) {
        assertOutPropertiesWithDelta(graph, 0, node, expected);
    }

    public static void assertInProperties(Graph graph, long node, double... expected) {
        assertInPropertiesWithDelta(graph, 0, node, expected);
    }

    public static void assertOutPropertiesWithDelta(Graph graph, double delta, long node, double... expected) {
        assertProperties(graph, Direction.OUTGOING, delta, node, expected);
    }

    public static void assertInPropertiesWithDelta(Graph graph, double delta, long node, double... expected) {
        assertProperties(graph, Direction.INCOMING, delta, node, expected);
    }

    private static void assertProperties(Graph graph, Direction direction, double delta, long node, double... expected) {
        LongArrayList idList = new LongArrayList(expected.length);
        DoubleArrayList properties = new DoubleArrayList(expected.length);
        graph.forEachRelationship(node, direction, 0D, (s, t, w) -> {
            idList.add(t);
            properties.add(w);
            return true;
        });
        long[] ids = idList.toArray();
        int[] order = IndirectSort.mergesort(0, ids.length, new AscendingLongComparator(ids));
        DoubleArrayList sortedProperties = new DoubleArrayList(ids.length);
        for (int index : order) {
            sortedProperties.add(properties.get(index));
        }
        assertArrayEquals(expected, sortedProperties.toArray(), delta);
    }

    public static void assertOutRelationships(Graph graph, long node, long... expected) {
        assertRelationships(graph, Direction.OUTGOING, node, expected);
    }

    public static void assertInRelationships(Graph graph, long node, long... expected) {
        assertRelationships(graph, Direction.INCOMING, node, expected);
    }

    private static void assertRelationships(Graph graph, Direction direction, long node, long... expected) {
        LongArrayList idList = new LongArrayList();
        graph.forEachRelationship(node, direction, (s, t) -> {
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
