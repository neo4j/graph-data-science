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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.SideEffectProcessingCounts;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.traversal.RandomWalkStreamConfig;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

class RandomWalkResultBuilderForStreamMode implements ResultBuilder<RandomWalkStreamConfig, Stream<long[]>, Stream<RandomWalkStreamResult>> {
    private final NodeLookup nodeLookup;
    private final boolean returnPath;

    RandomWalkResultBuilderForStreamMode(NodeLookup nodeLookup, boolean returnPath) {
        this.nodeLookup = nodeLookup;
        this.returnPath = returnPath;
    }

    @Override
    public Stream<RandomWalkStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        RandomWalkStreamConfig randomWalkStreamConfig,
        Optional<Stream<long[]>> result,
        AlgorithmProcessingTimings timings,
        SideEffectProcessingCounts counts
    ) {
        if (result.isEmpty()) return Stream.empty();

        Function<List<Long>, Path> pathCreator = returnPath
            ? (List<Long> nodes) -> PathFactory.create(nodeLookup, nodes, RelationshipType.withName("NEXT"))
            : (List<Long> nodes) -> null;

        return result.get()
            .map(nodes -> {
                var translatedNodes = translateInternalToNeoIds(nodes, graph);
                var path = pathCreator.apply(translatedNodes);
                return new RandomWalkStreamResult(translatedNodes, path);
            });
    }

    private List<Long> translateInternalToNeoIds(long[] nodes, IdMap idMap) {
        var translatedNodes = new ArrayList<Long>(nodes.length);
        for (int i = 0; i < nodes.length; i++) {
            translatedNodes.add(i, idMap.toOriginalNodeId(nodes[i]));
        }
        return translatedNodes;
    }
}
