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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.hits.HitsConfig;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class HitsResultBuilderForStreamMode implements StreamResultBuilder<PregelResult, HitsStreamResult> {

    private final HitsConfig configuration;

    public HitsResultBuilderForStreamMode(HitsConfig configuration){
        this.configuration = configuration;
    }
    @Override
    public Stream<HitsStreamResult> build(Graph graph, GraphStore graphStore, Optional<PregelResult> result) {
        if (result.isEmpty()) return Stream.empty();

        var auth = result.get().nodeValues().doubleProperties(configuration.authProperty());
        var hub = result.get().nodeValues().doubleProperties(configuration.hubProperty());

        return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
            .mapToObj(nodeId -> {
                // for every schema element
                var authValue = auth.get(nodeId);
                var hubValue = hub.get(nodeId);
                return new HitsStreamResult(graph.toOriginalNodeId(nodeId), Map.of(configuration.authProperty(),authValue,configuration.hubProperty(),hubValue));
            });
    }
}
