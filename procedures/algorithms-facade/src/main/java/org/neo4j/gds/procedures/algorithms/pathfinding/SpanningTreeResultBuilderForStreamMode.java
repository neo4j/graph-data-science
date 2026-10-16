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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeStreamConfig;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class SpanningTreeResultBuilderForStreamMode implements StreamResultBuilder<SpanningTree, SpanningTreeStreamResult> {

    private final SpanningTreeStreamConfig configuration;

    SpanningTreeResultBuilderForStreamMode(SpanningTreeStreamConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    public Stream<SpanningTreeStreamResult> build(
        NodeTranslator nodeTranslator,
        GraphStore graphStore,
        Optional<SpanningTree> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var spanningTree = result.get();

        var sourceNode = configuration.sourceNode();
        return LongStream.range(IdMap.START_NODE_ID, nodeTranslator.nodeCount())
            .filter(nodeId -> spanningTree.parent(nodeId) >= 0 || sourceNode == nodeTranslator.toOriginalNodeId(nodeId))
            .mapToObj(nodeId -> {
                var originalId = nodeTranslator.toOriginalNodeId(nodeId);
                return new SpanningTreeStreamResult(
                    originalId,
                    (sourceNode == originalId) ? sourceNode : nodeTranslator.toOriginalNodeId(spanningTree.parent(nodeId)),
                    spanningTree.costToParent(nodeId)
                );
            });
    }
}
