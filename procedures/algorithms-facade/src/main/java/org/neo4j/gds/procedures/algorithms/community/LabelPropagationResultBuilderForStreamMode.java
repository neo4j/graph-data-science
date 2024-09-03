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
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfig;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class LabelPropagationResultBuilderForStreamMode implements StreamResultBuilder<LabelPropagationResult, LabelPropagationStreamResult> {

    private final LabelPropagationStreamConfig configuration;

    LabelPropagationResultBuilderForStreamMode(LabelPropagationStreamConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    public Stream<LabelPropagationStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        Optional<LabelPropagationResult> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var labelPropagationResult = result.get();

        var nodePropertyValues = CommunityCompanion.nodePropertyValues(
            configuration.consecutiveIds(),
            NodePropertyValuesAdapter.adapt(labelPropagationResult.labels()),
            configuration.minCommunitySize(),
            configuration.concurrency()
        );

        return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
            .filter(nodePropertyValues::hasValue)
            .mapToObj(nodeId -> new LabelPropagationStreamResult(
                graph.toOriginalNodeId(nodeId),
                nodePropertyValues.longValue(nodeId)
            ));
    }
}
