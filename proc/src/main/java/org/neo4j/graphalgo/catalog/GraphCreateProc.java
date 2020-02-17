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
package org.neo4j.graphalgo.catalog;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.RelationshipProjectionMappings;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class GraphCreateProc extends CatalogProc {

    private static final String NO_GRAPH_NAME = "";

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
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphCreateConfig config = GraphCreateFromStoreConfig.of(
            getUsername(),
            graphName,
            nodeProjection,
            relationshipProjection,
            cypherConfig
        );
        validateConfig(cypherConfig, config);

        // computation
        GraphCreateResult result = runWithExceptionLogging(
            "Graph creation failed",
            () -> createGraph(config, HugeGraphFactory.class)
        );
        // result
        return Stream.of(result);
    }

    @Procedure(name = "gds.graph.create.estimate", mode = Mode.READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> createEstimate(
        @Name(value = "nodeProjection") @Nullable Object nodeProjection,
        @Name(value = "relationshipProjection") @Nullable Object relationshipProjection,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphCreateConfig config = GraphCreateFromStoreConfig.of(
            getUsername(),
            NO_GRAPH_NAME,
            nodeProjection,
            relationshipProjection,
            cypherConfig
        );
        validateConfig(cypherConfig, config);
        return estimateGraph(config, HugeGraphFactory.class);
    }

    @Procedure(name = "gds.graph.create.cypher", mode = Mode.READ)
    @Description("Creates a named graph in the catalog for use by algorithms.")
    public Stream<GraphCreateResult> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeQuery") String nodeQuery,
        @Name(value = "relationshipQuery") String relationshipQuery,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(getUsername(), graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphCreateFromCypherConfig config = GraphCreateFromCypherConfig.of(
            getUsername(),
            graphName,
            nodeQuery,
            relationshipQuery,
            cypherConfig
        );
        validateConfig(cypherConfig, config);

        // computation
        GraphCreateResult result = runWithExceptionLogging(
            "Graph creation failed",
            () -> createGraph(config, CypherGraphFactory.class)
        );
        // result
        return Stream.of(result);
    }

    @Procedure(name = "gds.graph.create.cypher.estimate", mode = Mode.READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> createCypherEstimate(
        @Name(value = "nodeQuery") String nodeQuery,
        @Name(value = "relationshipQuery") String relationshipQuery,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphCreateConfig config = GraphCreateFromCypherConfig.of(
            getUsername(),
            NO_GRAPH_NAME,
            nodeQuery,
            relationshipQuery,
            cypherConfig
        );
        validateConfig(cypherConfig, config);

        return estimateGraph(config, CypherGraphFactory.class);
    }

    private GraphCreateResult createGraph(GraphCreateConfig config, Class<? extends GraphFactory> factoryClazz) {
        GraphCreateResult.Builder builder = new GraphCreateResult.Builder(config);
        try (ProgressTimer ignored = ProgressTimer.start(builder::withCreateMillis)) {
            GraphLoader loader = newLoader(config, AllocationTracker.EMPTY);
            GraphFactory graphFactory = loader.build(factoryClazz);
            GraphFactory.ImportResult importResult = graphFactory.build();

            GraphsByRelationshipType graphs =  importResult.graphs();
            GraphDimensions dimensions = importResult.dimensions();
            GraphCreateConfig catalogConfig = config instanceof GraphCreateFromCypherConfig
                ? ((GraphCreateFromCypherConfig) config).inferProjections(dimensions)
                : config;

            builder
                .withGraph(graphs)
                .withNodeProjections(catalogConfig.nodeProjection())
                .withRelationshipProjections(catalogConfig.relationshipProjection());

            GraphCatalog.set(catalogConfig, graphs);
        }

        return builder.build();
    }

    private Stream<MemoryEstimateResult> estimateGraph(GraphCreateConfig config, Class<? extends GraphFactory> factoryClazz) {
        GraphLoader loader = newLoader(config, AllocationTracker.EMPTY);
        GraphFactory graphFactory = loader.build(factoryClazz);
        GraphDimensions dimensions = updateDimensions(config, graphFactory, graphFactory.dimensions());

        MemoryTree memoryTree = estimate(graphFactory, dimensions, config);
        return Stream.of(new MemoryEstimateResult(new MemoryTreeWithDimensions(memoryTree, dimensions)));
    }

    private GraphDimensions updateDimensions(
        GraphCreateConfig config,
        GraphFactory graphFactory,
        GraphDimensions dimensions
    ) {
        if (config.nodeCount() > -1) {
            dimensions = ImmutableGraphDimensions.builder()
                .from(graphFactory.dimensions())
                .nodeCount(config.nodeCount())
                .highestNeoId(config.nodeCount())
                .relationshipProjectionMappings(RelationshipProjectionMappings.all())
                .maxRelCount(Math.max(config.relationshipCount(), 0))
                .build();
        }
        return dimensions;
    }

    public MemoryTree estimate(GraphFactory factory, GraphDimensions dimensions, GraphCreateConfig config) {
        return factory.memoryEstimation(dimensions).estimate(dimensions, config.concurrency());
    }

    public static class GraphCreateResult {

        public final String graphName;
        public final Map<String, Object> nodeProjection;
        public final Map<String, Object> relationshipProjection;
        public final long nodeCount;
        public final long relationshipCount;
        public final long createMillis;

        GraphCreateResult(
            String graphName,
            Map<String, Object> nodeProjection,
            Map<String, Object> relationshipProjection,
            long nodeCount,
            long relationshipCount,
            long createMillis
        ) {
            this.graphName = graphName;
            this.nodeProjection = nodeProjection;
            this.relationshipProjection = relationshipProjection;
            this.nodeCount = nodeCount;
            this.relationshipCount = relationshipCount;
            this.createMillis = createMillis;
        }

        static final class Builder {
            private final String graphName;
            private NodeProjections nodeProjections;
            private RelationshipProjections relationshipProjections;
            private long nodeCount;
            private long relationshipCount;
            private long createMillis;

            Builder(GraphCreateConfig config) {
                this.graphName = config.graphName();
                this.nodeProjections = config.nodeProjection();
                this.relationshipProjections = config.relationshipProjection();
            }

            Builder withGraph(GraphsByRelationshipType graph) {
                relationshipCount = graph.relationshipCount();
                nodeCount = graph.nodeCount();
                return this;
            }

            Builder withCreateMillis(long createMillis) {
                this.createMillis = createMillis;
                return this;
            }

            Builder withNodeProjections(NodeProjections nodeProjections) {
                this.nodeProjections = nodeProjections;
                return this;
            }

            Builder withRelationshipProjections(RelationshipProjections relationshipProjections) {
                this.relationshipProjections = relationshipProjections;
                return this;
            }

            GraphCreateResult build() {
                return new GraphCreateResult(
                    graphName,
                    nodeProjections.toObject(),
                    relationshipProjections.toObject(),
                    nodeCount,
                    relationshipCount,
                    createMillis
                );
            }
        }
    }
}
