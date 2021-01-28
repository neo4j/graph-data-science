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
package org.neo4j.graphalgo.catalog;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphCreateProc extends CatalogProc {

    private static final String NO_GRAPH_NAME = "";
    private static final String DESCRIPTION = "Creates a named graph in the catalog for use by algorithms.";

    @Procedure(name = "gds.graph.create", mode = READ)
    @Description(DESCRIPTION)
    public Stream<GraphCreateNativeResult> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProjection") @Nullable Object nodeProjection,
        @Name(value = "relationshipProjection") @Nullable Object relationshipProjection,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(username(), graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphCreateFromStoreConfig config = GraphCreateFromStoreConfig.of(
            username(),
            graphName,
            nodeProjection,
            relationshipProjection,
            cypherConfig
        );
        validateConfig(cypherConfig, config);

        // computation
        GraphCreateNativeResult result = runWithExceptionLogging(
            "Graph creation failed",
            () -> (GraphCreateNativeResult) createGraph(config)
        );
        // result
        return Stream.of(result);
    }

    @Procedure(name = "gds.graph.create.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> createEstimate(
        @Name(value = "nodeProjection") @Nullable Object nodeProjection,
        @Name(value = "relationshipProjection") @Nullable Object relationshipProjection,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphCreateConfig config = GraphCreateFromStoreConfig.of(
            username(),
            NO_GRAPH_NAME,
            nodeProjection,
            relationshipProjection,
            cypherConfig
        );
        validateConfig(cypherConfig, config);
        return estimateGraph(config);
    }

    @Procedure(name = "gds.graph.create.cypher", mode = READ)
    @Description(DESCRIPTION)
    public Stream<GraphCreateCypherResult> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeQuery") String nodeQuery,
        @Name(value = "relationshipQuery") String relationshipQuery,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(username(), graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphCreateFromCypherConfig config = GraphCreateFromCypherConfig.of(
            username(),
            graphName,
            nodeQuery,
            relationshipQuery,
            cypherConfig
        );
        validateConfig(cypherConfig, config);

        // computation
        GraphCreateCypherResult result = runWithExceptionLogging(
            "Graph creation failed",
            () -> (GraphCreateCypherResult) createGraph(config)
        );
        // result
        return Stream.of(result);
    }

    @Procedure(name = "gds.graph.create.cypher.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> createCypherEstimate(
        @Name(value = "nodeQuery") String nodeQuery,
        @Name(value = "relationshipQuery") String relationshipQuery,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphCreateFromCypherConfig config = GraphCreateFromCypherConfig.of(
            username(),
            NO_GRAPH_NAME,
            nodeQuery,
            relationshipQuery,
            cypherConfig
        );

        validateConfig(cypherConfig, config);
        return estimateGraph(config);
    }

    /**
     * This is (temporarily) overridden due to a performance regression
     * caused by tracking memory allocation during graph creation.
     */
    @Override
    protected AllocationTracker allocationTracker() {
        return AllocationTracker.empty();
    }

    private GraphCreateResult createGraph(GraphCreateConfig config) {
        tryValidateMemoryUsage(config, this::memoryTreeWithDimensions);

        GraphCreateResult.Builder builder = config instanceof GraphCreateFromCypherConfig
            ? new GraphCreateCypherResult.Builder((GraphCreateFromCypherConfig) config)
            : new GraphCreateNativeResult.Builder((GraphCreateFromStoreConfig) config);

        try (ProgressTimer ignored = ProgressTimer.start(builder::withCreateMillis)) {
            GraphLoader loader = newLoader(config, allocationTracker());
            GraphStore graphStore = loader.graphStore();

            builder
                .withNodeCount(graphStore.nodeCount())
                .withRelationshipCount(graphStore.relationshipCount());

            GraphStoreCatalog.set(config, graphStore);
        }

        return builder.build();
    }

    private Stream<MemoryEstimateResult> estimateGraph(GraphCreateConfig config) {
        return Stream.of(new MemoryEstimateResult(memoryTreeWithDimensions(config)));
    }

    MemoryTreeWithDimensions memoryTreeWithDimensions(GraphCreateConfig config) {
        var memoryEstimationAndDimensions = estimateGraphCreate(config);
        MemoryTree memoryTree = memoryEstimationAndDimensions.memoryEstimation().estimate(memoryEstimationAndDimensions.graphDimensions(), config.readConcurrency());
        return new MemoryTreeWithDimensions(memoryTree, memoryEstimationAndDimensions.graphDimensions());
    }

    public static class GraphCreateResult {
        public final String graphName;
        public final long nodeCount;
        public final long relationshipCount;
        public final long createMillis;

        GraphCreateResult(
            String graphName,
            long nodeCount,
            long relationshipCount,
            long createMillis
        ) {
            this.graphName = graphName;
            this.nodeCount = nodeCount;
            this.relationshipCount = relationshipCount;
            this.createMillis = createMillis;
        }

        protected abstract static class Builder {
            final String graphName;
            long nodeCount;
            long relationshipCount;
            long createMillis;

            Builder(GraphCreateConfig config) {
                this.graphName = config.graphName();
            }

            Builder withNodeCount(long nodeCount) {
                this.nodeCount = nodeCount;
                return this;
            }

            Builder withRelationshipCount(long relationshipCount) {
                this.relationshipCount = relationshipCount;
                return this;
            }

            Builder withCreateMillis(long createMillis) {
                this.createMillis = createMillis;
                return this;
            }

            abstract GraphCreateResult build();
        }
    }

    public static class GraphCreateNativeResult extends GraphCreateResult {

        public final Map<String, Object> nodeProjection;
        public final Map<String, Object> relationshipProjection;

        GraphCreateNativeResult(
            String graphName,
            Map<String, Object> nodeProjection,
            Map<String, Object> relationshipProjection,
            long nodeCount,
            long relationshipCount,
            long createMillis
        ) {
            super(graphName, nodeCount, relationshipCount, createMillis);
            this.nodeProjection = nodeProjection;
            this.relationshipProjection = relationshipProjection;
        }

        protected static final class Builder extends GraphCreateResult.Builder {
            private NodeProjections nodeProjections;
            private RelationshipProjections relationshipProjections;

            Builder(GraphCreateFromStoreConfig config) {
                super(config);
                this.nodeProjections = config.nodeProjections();
                this.relationshipProjections = config.relationshipProjections();
            }

            GraphCreateNativeResult build() {
                return new GraphCreateNativeResult(
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

    public static class GraphCreateCypherResult extends GraphCreateResult {
        public final String nodeQuery;
        public final String relationshipQuery;

        GraphCreateCypherResult(
            String graphName,
            String nodeQuery,
            String relationshipQuery,
            long nodeCount,
            long relationshipCount,
            long createMillis
        ) {
            super(graphName, nodeCount, relationshipCount, createMillis);
            this.nodeQuery = nodeQuery;
            this.relationshipQuery = relationshipQuery;
        }

        protected static final class Builder extends GraphCreateResult.Builder {
            private String nodeQuery;
            private String relationshipQuery;

            Builder(GraphCreateFromCypherConfig config) {
                super(config);
                this.nodeQuery = config.nodeQuery();
                this.relationshipQuery = config.relationshipQuery();
            }

            GraphCreateCypherResult build() {
                return new GraphCreateCypherResult(
                    graphName,
                    nodeQuery,
                    relationshipQuery,
                    nodeCount,
                    relationshipCount,
                    createMillis
                );
            }
        }
    }
}
