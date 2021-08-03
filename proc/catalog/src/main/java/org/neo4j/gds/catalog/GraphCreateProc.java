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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.filter.GraphStoreFilter;
import org.neo4j.gds.beta.filter.expression.SemanticErrors;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.GraphCreateFromCypherConfig;
import org.neo4j.gds.config.GraphCreateFromGraphConfig;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.utils.ExceptionUtil;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphCreateProc extends CatalogProc {

    private static final String NO_GRAPH_NAME = "";
    private static final String DESCRIPTION = "Creates a named graph in the catalog for use by algorithms.";

    private static final Set<String> DISALLOWED_CONFIG_KEYS = Set.of(
        GraphCreateFromStoreConfig.NODE_PROJECTION_KEY,
        GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY,
        GraphCreateFromCypherConfig.NODE_QUERY_KEY,
        GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY
    );

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

    @Procedure(name = "gds.beta.graph.create.subgraph", mode = READ)
    @Description(DESCRIPTION)
    public Stream<GraphCreateSubgraphResult> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "fromGraphName") String fromGraphName,
        @Name(value = "nodeFilter") String nodeFilter,
        @Name(value = "relationshipFilter") String relationshipFilter,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(username(), graphName);

        var procedureConfig = CypherMapWrapper.create(configuration);

        var fromGraphStore = graphStoreFromCatalog(fromGraphName);

        var graphCreateConfig = GraphCreateFromGraphConfig.of(
            username(),
            graphName,
            fromGraphName,
            nodeFilter,
            relationshipFilter,
            fromGraphStore.config(),
            procedureConfig
        );

        validateConfig(procedureConfig, graphCreateConfig);

        GraphCreateSubgraphResult result = runWithExceptionLogging(
            "Graph creation failed",
            ExceptionUtil.supplier(() -> createGraphFromGraphStore(fromGraphStore.graphStore(), graphCreateConfig))
        );

        return Stream.of(result);
    }

    private GraphCreateSubgraphResult createGraphFromGraphStore(
        GraphStore fromGraphStore,
        GraphCreateFromGraphConfig config
    ) throws
        ParseException,
        SemanticErrors {

        var progressTimer = ProgressTimer.start();

        var graphStore = GraphStoreFilter.filter(
            fromGraphStore,
            config,
            Pools.DEFAULT,
            log,
            allocationTracker()
        );

        GraphStoreCatalog.set(config, graphStore);

        var createMillis = progressTimer.stop().getDuration();

        return new GraphCreateSubgraphResult(
            config.graphName(),
            config.fromGraphName(),
            config.nodeFilter(),
            config.relationshipFilter(),
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            createMillis
        );
    }

    private void validateConfig(CypherMapWrapper cypherConfig, GraphCreateConfig createConfig) {
        var allowedKeys = createConfig.isFictitiousLoading()
            ? createConfig.configKeys()
            : createConfig.configKeys()
                .stream()
                .filter(key -> !DISALLOWED_CONFIG_KEYS.contains(key))
                .collect(Collectors.toList());

        validateConfig(cypherConfig, allowedKeys);
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

    @SuppressWarnings("unused")
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

    @SuppressWarnings("unused")
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
            private final NodeProjections nodeProjections;
            private final RelationshipProjections relationshipProjections;

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

    @SuppressWarnings("unused")
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
            private final String nodeQuery;
            private final String relationshipQuery;

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

    @SuppressWarnings("unused")
    public static class GraphCreateSubgraphResult extends GraphCreateResult {
        public final String fromGraphName;
        public final String nodeFilter;
        public final String relationshipFilter;

        GraphCreateSubgraphResult(
            String graphName,
            String fromGraphName,
            String nodeFilter,
            String relationshipFilter,
            long nodeCount,
            long relationshipCount,
            long createMillis
        ) {
            super(graphName, nodeCount, relationshipCount, createMillis);
            this.fromGraphName = fromGraphName;
            this.nodeFilter = nodeFilter;
            this.relationshipFilter = relationshipFilter;
        }
    }
}
