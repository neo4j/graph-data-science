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
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.louvain.LouvainStreamConfig;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class LouvainResultBuilderForStreamMode implements StreamResultBuilder<LouvainResult, LouvainStreamResult> {

    private final LouvainStreamConfig configuration;

    LouvainResultBuilderForStreamMode(LouvainStreamConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    public Stream<LouvainStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        Optional<LouvainResult> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var louvainResult = result.get();

        var nodePropertyValues = CommunityCompanion.nodePropertyValues(
            configuration.consecutiveIds(),
            NodePropertyValuesAdapter.adapt(louvainResult.dendrogramManager().getCurrent()),
            configuration.minCommunitySize(),
            configuration.concurrency()
        );

        return LongStream.range(0, graph.nodeCount())
            .boxed()
            .filter(nodePropertyValues::hasValue)
            .map(nodeId -> {
                var communities = configuration.includeIntermediateCommunities()
                    ? louvainResult.intermediateCommunities(nodeId)
                    : null;
                var communityId = nodePropertyValues.longValue(nodeId);
                return LouvainStreamResult.create(graph.toOriginalNodeId(nodeId), communities, communityId);
            });
    }
}
