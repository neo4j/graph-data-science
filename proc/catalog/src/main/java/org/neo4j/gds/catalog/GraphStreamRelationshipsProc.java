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

import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.gds.ProcPreconditions;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphStreamRelationshipsConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
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
        var relationshipTypesAndGraphs = config.relationshipTypeIdentifiers(graphStore).stream()
            .map(relationshipType -> Tuples.pair(relationshipType.name(), graphStore.getGraph(relationshipType)))
            .collect(Collectors.toList());

        return ParallelUtil.parallelStream(
            LongStream.range(0, graphStore.nodeCount()),
            config.concurrency(),
            nodeStream -> nodeStream
                .boxed()
                .flatMap(nodeId -> relationshipTypesAndGraphs.stream().flatMap(graphAndRelationshipType -> {
                    var relationshipType = graphAndRelationshipType.getOne();
                    Graph graph = graphAndRelationshipType.getTwo();

                    var originalSourceId = graph.toOriginalNodeId(nodeId);

                    return graph
                        .streamRelationships(nodeId, Double.NaN)
                        .map(relationshipCursor -> new TopologyResult(originalSourceId, graph.toOriginalNodeId(relationshipCursor.targetId()), relationshipType));
                }))
        );
    }

    public static class TopologyResult {
        public final long sourceNodeId;
        public final long targetNodeId;
        public final String relationshipType;

        public TopologyResult(long sourceNodeId, long targetNodeId, String relationshipType) {
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.relationshipType = relationshipType;
        }

        @Override
        public String toString() {
            return formatWithLocale("TopologyResult(%d, %d, type: %s)", sourceNodeId, targetNodeId, relationshipType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopologyResult that = (TopologyResult) o;
            return sourceNodeId == that.sourceNodeId && targetNodeId == that.targetNodeId && relationshipType.equals(
                that.relationshipType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceNodeId, targetNodeId, relationshipType);
        }
    }
}
