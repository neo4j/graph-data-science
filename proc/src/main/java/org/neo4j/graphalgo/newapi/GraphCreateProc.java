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

package org.neo4j.graphalgo.newapi;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class GraphCreateProc extends BaseProc {

    static final String HISTOGRAM_FIELD_NAME = "histogram";

    @Procedure(name = "algo.beta.graph.create", mode = Mode.READ)
    @Description("CALL graph.create(" +
                 "  graphName: STRING," +
                 "  nodeProjection: MAP," +
                 "  relationshipProjection: MAP," +
                 "  configuration: MAP" +
                 ") YIELD" +
                 "  graphName: STRING," +
                 "  nodeProjection: MAP," +
                 "  relationshipProjection: MAP," +
                 "  nodes: INTEGER," +
                 "  relationships: INTEGER," +
                 "  createMillis: INTEGER")
    public Stream<GraphCreateResult> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProjection") @Nullable Object nodeProjection,
        @Name(value = "relationshipProjection") @Nullable Object relationshipProjection,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        CypherMapWrapper.failOnBlank("graphName", graphName);

        // input
        GraphCreateConfig config = GraphCreateConfig.of(
            getUsername(),
            graphName,
            nodeProjection,
            relationshipProjection,
            CypherMapWrapper.create(configuration)
        );
        // computation
        GraphCreateResult result = runWithExceptionLogging(
            "Graph creation failed",
            () -> createGraph(config)
        );
        // result
        return Stream.of(result);
    }

    private GraphCreateResult createGraph(GraphCreateConfig config) {
        if (GraphCatalog.exists(config.username(), config.graphName())) {
            throw new IllegalArgumentException(String.format(
                "A graph with name '%s' already exists.",
                config.graphName()
            ));
        }

        GraphCreateResult.Builder builder = new GraphCreateResult.Builder(config);
        try (ProgressTimer ignored = ProgressTimer.start(builder::withCreateMillis)) {
            ResolvedPropertyMappings propertyMappings = ResolvedPropertyMappings.empty();

            GraphLoader loader = newLoader(AllocationTracker.EMPTY, config);
            HugeGraphFactory graphFactory = loader.build(HugeGraphFactory.class);
            GraphsByRelationshipType graphFromType =
                !config.relationshipProjection().isEmpty() || propertyMappings.hasMappings()
                    ? graphFactory.importAllGraphs()
                    : GraphsByRelationshipType.of(graphFactory.build());

            builder.withGraph(graphFromType);
            GraphCatalog.set(config, graphFromType);
        }

        return builder.build();
    }

    public static class GraphCreateResult {

        public final String graphName;
        public final Map<String, Object> nodeProjection, relationshipProjection;
        public final long nodes, relationships, createMillis;

        GraphCreateResult(
            String graphName,
            Map<String, Object> nodeProjection,
            Map<String, Object> relationshipProjection,
            long nodes,
            long relationships,
            long createMillis
        ) {
            this.graphName = graphName;
            this.nodeProjection = nodeProjection;
            this.relationshipProjection = relationshipProjection;
            this.nodes = nodes;
            this.relationships = relationships;
            this.createMillis = createMillis;
        }

        static final class Builder {
            private final String graphName;
            private final NodeProjections nodeProjections;
            private final RelationshipProjections relationshipProjections;
            private long nodes, relationships, createMillis;

            Builder(GraphCreateConfig config) {
                this.graphName = config.graphName();
                nodeProjections = config.nodeProjection();
                relationshipProjections = config.relationshipProjection();
            }

            void withGraph(GraphsByRelationshipType graph) {
                relationships = graph.relationshipCount();
                nodes = graph.nodeCount();
            }

            void withCreateMillis(long createMillis) {
                this.createMillis = createMillis;
            }

            GraphCreateResult build() {
                return new GraphCreateResult(
                    graphName,
                    nodeProjections.toObject(),
                    relationshipProjections.toObject(),
                    nodes,
                    relationships,
                    createMillis
                );
            }
        }
    }
}
