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
package org.neo4j.gds.catalog;

import org.neo4j.gds.ProcPreconditions;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphStreamRelationshipsConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphStreamRelationshipsProc extends CatalogProc {

    @Procedure(name = "gds.beta.graph.relationships.stream", mode = READ)
    @Description("Streams the given relationship source/target pairs")
    public Stream<TopologyResult> streamRelationships(
        @Name(value = "graphName") String graphName,
        @Name(value = "relationshipTypes", defaultValue = "['*']") List<String> relationshipTypes,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        validateGraphName(graphName);

        var cypherMapWrapper = CypherMapWrapper.create(configuration);
        var config = GraphStreamRelationshipsConfig.of(
            graphName,
            relationshipTypes,
            cypherMapWrapper
        );

        validateConfig(cypherMapWrapper, config);
        var graphStore = graphStoreFromCatalog(graphName, config).graphStore();
        config.validate(graphStore);

        return streamRelationshipTopology(graphStore, config);
    }

    private static Stream<TopologyResult> streamRelationshipTopology(GraphStore graphStore, GraphStreamRelationshipsConfig config) {
        var graph = graphStore.getGraph(config
            .relationshipTypeIdentifiers(graphStore)
            .toArray(RelationshipType[]::new));

        return ParallelUtil.parallelStream(
            LongStream.range(0, graph.nodeCount()),
            config.concurrency(),
            nodeStream -> nodeStream
                .boxed()
                .flatMap(nodeId -> graph.streamRelationships(nodeId, Double.NaN))
                .map(relationshipCursor -> new TopologyResult(relationshipCursor.sourceId(), relationshipCursor.targetId()))
        );
    }

    public static class TopologyResult {
        public final long sourceNodeId;
        public final long targetNodeId;

        public TopologyResult(long sourceNodeId, long targetNodeId) {
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
        }
    }
}
