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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfig;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class K1ColoringResultBuilderForStreamMode implements StreamResultBuilder<K1ColoringStreamConfig, K1ColoringResult, K1ColoringStreamResult> {

    @Override
    public Stream<K1ColoringStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        K1ColoringStreamConfig configuration,
        Optional<K1ColoringResult> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var k1ColoringResult = result.get();

        var nodePropertyValues = CommunityCompanion.nodePropertyValues(
            false,
            NodePropertyValuesAdapter.adapt(k1ColoringResult.colors()),
            configuration.minCommunitySize(),
            configuration.concurrency()
        );

        return LongStream
            .range(IdMap.START_NODE_ID, graph.nodeCount())
            .filter(nodePropertyValues::hasValue)
            .mapToObj(nodeId -> new K1ColoringStreamResult(
                    graph.toOriginalNodeId(nodeId),
                    nodePropertyValues.longValue(nodeId)
                )
            );
    }
}
