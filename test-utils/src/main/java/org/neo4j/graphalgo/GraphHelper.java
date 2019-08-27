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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

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

    public static double[] collectTargetWeights(final Graph graph, long sourceId) {
        DoubleStream.Builder outWeights = DoubleStream.builder();
        graph.forEachRelationship(graph.toMappedNodeId(sourceId), Direction.OUTGOING,
                (sourceNodeId, targetNodeId, weight) -> {
                    outWeights.add(weight);
                    return true;
                });
        return outWeights.build().toArray();
    }

    private GraphHelper() {
        throw new UnsupportedOperationException("No instances");
    }
}
