/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.ModernGraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
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

public class GraphCreateProc extends CatalogProc {

    @Procedure(name = "gds.graph.create", mode = Mode.READ)
    @Description("Creates a named graph in the catalog for use by algorithms.")
    public Stream<GraphCreateResult> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProjection") @Nullable Object nodeProjection,
        @Name(value = "relationshipProjection") @Nullable Object relationshipProjection,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(getUsername(), graphName);

        // input
        GraphCreateConfig config = GraphCreateFromStoreConfig.of(
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
        GraphCreateResult.Builder builder = new GraphCreateResult.Builder(config);
        try (ProgressTimer ignored = ProgressTimer.start(builder::withCreateMillis)) {
            ResolvedPropertyMappings propertyMappings = ResolvedPropertyMappings.empty();

            ModernGraphLoader loader = newLoader(config, AllocationTracker.EMPTY);
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

    @Procedure(name = "gds.graph.create.cypher", mode = Mode.READ)
    @Description("Creates a named graph in the catalog for use by algorithms.")
    public Stream<GraphCreateCypherResult> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeQuery") String nodeQuery,
        @Name(value = "relationshipQuery") String relationshipQuery,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(getUsername(), graphName);

        // input
        GraphCreateFromCypherConfig config = GraphCreateFromCypherConfig.of(
            getUsername(),
            graphName,
            nodeQuery,
            relationshipQuery,
            CypherMapWrapper.create(configuration)
        );

        // computation
        GraphCreateCypherResult result = runWithExceptionLogging(
            "Graph creation failed",
            () -> createCypherGraph(config)
        );
        // result
        return Stream.of(result);
    }

    private GraphCreateCypherResult createCypherGraph(GraphCreateFromCypherConfig config) {
        GraphCreateCypherResult.Builder builder = new GraphCreateCypherResult.Builder(config);
        try (ProgressTimer ignored = ProgressTimer.start(builder::withCreateMillis)) {
            ModernGraphLoader loader = newLoader(config, AllocationTracker.EMPTY);
            CypherGraphFactory graphFactory = loader.build(CypherGraphFactory.class);
            GraphsByRelationshipType graphFromType =
                !config.relationshipProperties().hasMappings()
                    ? graphFactory.importAllGraphs()
                    : GraphsByRelationshipType.of(graphFactory.build());

            builder.withGraph(graphFromType);

            GraphCreateConfig graphCreateConfig = new GraphCreateFromStoreConfigImpl(
                NodeProjections.of(),
                // TODO: convert GraphsByRelationshipType to RelationshipProjection in GraphCreateConfig
                RelationshipProjections.of(),
                config.graphName(),
                PropertyMappings.of(),
                PropertyMappings.of(),
                config.concurrency(),
                config.username()
            );

            GraphCatalog.set(graphCreateConfig, graphFromType);
        }

        return builder.build();
    }

    private void validateGraphName(String username, String graphName) {
        CypherMapWrapper.failOnBlank("graphName", graphName);
        if (GraphCatalog.exists(username, graphName)) {
            throw new IllegalArgumentException(String.format(
                "A graph with name '%s' already exists.",
                graphName
            ));
        }
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

    public static class GraphCreateCypherResult {

        public final String graphName;
        public final String nodeQuery;
        public final String relationshipQuery;
        public final long nodes, relationships, createMillis;

        GraphCreateCypherResult(
            String graphName,
            String nodeQuery,
            String relationshipQuery,
            long nodes,
            long relationships,
            long createMillis
        ) {
            this.graphName = graphName;
            this.nodeQuery = nodeQuery;
            this.relationshipQuery = relationshipQuery;
            this.nodes = nodes;
            this.relationships = relationships;
            this.createMillis = createMillis;
        }

        static final class Builder {
            private final String graphName;
            private final String nodeQuery;
            private final String relationshipQuery;
            private long nodes, relationships, createMillis;

            Builder(GraphCreateFromCypherConfig config) {
                this.graphName = config.graphName();
                this.nodeQuery = config.nodeQuery();
                this.relationshipQuery = config.relationshipQuery();
            }

            void withGraph(GraphsByRelationshipType graph) {
                this.relationships = graph.relationshipCount();
                this.nodes = graph.nodeCount();
            }

            void withCreateMillis(long createMillis) {
                this.createMillis = createMillis;
            }

            GraphCreateCypherResult build() {
                return new GraphCreateCypherResult(
                    graphName,
                    nodeQuery,
                    relationshipQuery,
                    nodes,
                    relationships,
                    createMillis
                );
            }
        }
    }
}
