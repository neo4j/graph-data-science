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
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.filter.GraphStoreFilter;
import org.neo4j.gds.beta.filter.expression.SemanticErrors;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.config.GraphProjectFromGraphConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.FictitiousGraphStoreLoader;
import org.neo4j.gds.executor.GraphStoreCreator;
import org.neo4j.gds.executor.GraphStoreFromDatabaseLoader;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.gds.results.MemoryEstimateResult;
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

public class GraphProjectProc extends CatalogProc {

    private static final String NO_GRAPH_NAME = "";
    private static final String DESCRIPTION = "Creates a named graph in the catalog for use by algorithms.";

    private static final Set<String> DISALLOWED_CONFIG_KEYS = Set.of(
        GraphProjectFromStoreConfig.NODE_PROJECTION_KEY,
        GraphProjectFromStoreConfig.RELATIONSHIP_PROJECTION_KEY,
        GraphProjectFromCypherConfig.NODE_QUERY_KEY,
        GraphProjectFromCypherConfig.RELATIONSHIP_QUERY_KEY
    );

    @Procedure(name = "gds.graph.project", mode = READ)
    @Description(DESCRIPTION)
    public Stream<GraphProjectNativeResult> project(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeProjection") @Nullable Object nodeProjection,
        @Name(value = "relationshipProjection") @Nullable Object relationshipProjection,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        validateGraphName(username(), graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphProjectFromStoreConfig config = GraphProjectFromStoreConfig.of(
            username(),
            graphName,
            nodeProjection,
            relationshipProjection,
            cypherConfig
        );
        validateConfig(cypherConfig, config);

        // computation
        GraphProjectNativeResult result = runWithExceptionLogging(
            "Graph creation failed",
            () -> (GraphProjectNativeResult) projectGraph(config)
        );
        // result
        return Stream.of(result);
    }

    @Procedure(name = "gds.graph.project.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> projectEstimate(
        @Name(value = "nodeProjection") @Nullable Object nodeProjection,
        @Name(value = "relationshipProjection") @Nullable Object relationshipProjection,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();

        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphProjectConfig config = GraphProjectFromStoreConfig.of(
            username(),
            NO_GRAPH_NAME,
            nodeProjection,
            relationshipProjection,
            cypherConfig
        );
        validateConfig(cypherConfig, config);
        return estimateGraph(config);
    }

    @Procedure(name = "gds.graph.project.cypher", mode = READ)
    @Description(DESCRIPTION)
    public Stream<GraphProjectCypherResult> projectCypher(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeQuery") String nodeQuery,
        @Name(value = "relationshipQuery") String relationshipQuery,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        validateGraphName(username(), graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphProjectFromCypherConfig config = GraphProjectFromCypherConfig.of(
            username(),
            graphName,
            nodeQuery,
            relationshipQuery,
            cypherConfig
        );
        validateConfig(cypherConfig, config);

        // computation
        GraphProjectCypherResult result = runWithExceptionLogging(
            "Graph creation failed",
            () -> (GraphProjectCypherResult) projectGraph(config)
        );
        // result
        return Stream.of(result);
    }

    @Procedure(name = "gds.graph.project.cypher.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> projectCypherEstimate(
        @Name(value = "nodeQuery") String nodeQuery,
        @Name(value = "relationshipQuery") String relationshipQuery,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();

        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        GraphProjectFromCypherConfig config = GraphProjectFromCypherConfig.of(
            username(),
            NO_GRAPH_NAME,
            nodeQuery,
            relationshipQuery,
            cypherConfig
        );

        validateConfig(cypherConfig, config);
        return estimateGraph(config);
    }

    @Procedure(name = "gds.beta.graph.project.subgraph", mode = READ)
    @Description(DESCRIPTION)
    public Stream<GraphProjectSubgraphResult> projectSubgraph(
        @Name(value = "graphName") String graphName,
        @Name(value = "fromGraphName") String fromGraphName,
        @Name(value = "nodeFilter") String nodeFilter,
        @Name(value = "relationshipFilter") String relationshipFilter,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        validateGraphName(username(), graphName);

        var procedureConfig = CypherMapWrapper.create(configuration);

        var fromGraphStore = graphStoreFromCatalog(fromGraphName);

        var graphProjectConfig = GraphProjectFromGraphConfig.of(
            username(),
            graphName,
            fromGraphName,
            nodeFilter,
            relationshipFilter,
            fromGraphStore.config(),
            procedureConfig
        );

        validateConfig(procedureConfig, graphProjectConfig);

        GraphProjectSubgraphResult result = runWithExceptionLogging(
            "Graph creation failed",
            ExceptionUtil.supplier(() -> projectGraphFromGraphStore(fromGraphStore.graphStore(), graphProjectConfig))
        );

        return Stream.of(result);
    }

    private GraphProjectSubgraphResult projectGraphFromGraphStore(
        GraphStore fromGraphStore,
        GraphProjectFromGraphConfig config
    ) throws
        ParseException,
        SemanticErrors {

        var progressTimer = ProgressTimer.start();

        var progressTracker = new TaskProgressTracker(
            GraphStoreFilter.progressTask(fromGraphStore),
            executionContext().log(),
            config.concurrency(),
            config.jobId(),
            executionContext().taskRegistryFactory(),
            EmptyUserLogRegistryFactory.INSTANCE
        );

        var graphStore = GraphStoreFilter.filter(
            fromGraphStore,
            config,
            Pools.DEFAULT,
            progressTracker
        );

        GraphStoreCatalog.set(config, graphStore);

        var projectMillis = progressTimer.stop().getDuration();

        return new GraphProjectSubgraphResult(
            config.graphName(),
            config.fromGraphName(),
            config.nodeFilter(),
            config.relationshipFilter(),
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            projectMillis
        );
    }

    private void validateConfig(CypherMapWrapper cypherConfig, GraphProjectConfig graphProjectConfig) {
        var allowedKeys = graphProjectConfig.isFictitiousLoading()
            ? graphProjectConfig.configKeys()
            : graphProjectConfig.configKeys()
                .stream()
                .filter(key -> !DISALLOWED_CONFIG_KEYS.contains(key))
                .collect(Collectors.toList());

        validateConfig(cypherConfig, allowedKeys);
    }

    private GraphProjectResult projectGraph(GraphProjectConfig config) {
        memoryUsageValidator().tryValidateMemoryUsage(config, this::memoryTreeWithDimensions);

        GraphProjectResult.Builder builder = config instanceof GraphProjectFromCypherConfig
            ? new GraphProjectCypherResult.Builder((GraphProjectFromCypherConfig) config)
            : new GraphProjectNativeResult.Builder((GraphProjectFromStoreConfig) config);

        try (ProgressTimer ignored = ProgressTimer.start(builder::withProjectMillis)) {
            GraphStore graphStore = new GraphStoreFromDatabaseLoader(
                config,
                username(),
                graphLoaderContext()
            ).graphStore();

            builder
                .withNodeCount(graphStore.nodeCount())
                .withRelationshipCount(graphStore.relationshipCount());

            GraphStoreCatalog.set(config, graphStore);
        }

        return builder.build();
    }

    private Stream<MemoryEstimateResult> estimateGraph(GraphProjectConfig config) {
        return Stream.of(new MemoryEstimateResult(memoryTreeWithDimensions(config)));
    }

    MemoryTreeWithDimensions memoryTreeWithDimensions(GraphProjectConfig config) {
        GraphStoreCreator graphStoreCreator;
        if (config.isFictitiousLoading()) {
            graphStoreCreator = new FictitiousGraphStoreLoader(config);
        } else {
            graphStoreCreator = new GraphStoreFromDatabaseLoader(
                config,
                username(),
                graphLoaderContext()
            );
        }
        var graphDimensions = graphStoreCreator.graphDimensions();

        MemoryTree memoryTree = graphStoreCreator
            .estimateMemoryUsageDuringLoading()
            .estimate(graphDimensions, config.readConcurrency());

        return new MemoryTreeWithDimensions(memoryTree, graphDimensions);
    }

    @SuppressWarnings("unused")
    public static class GraphProjectResult {
        public final String graphName;
        public final long nodeCount;
        public final long relationshipCount;
        public final long projectMillis;

        GraphProjectResult(
            String graphName,
            long nodeCount,
            long relationshipCount,
            long projectMillis
        ) {
            this.graphName = graphName;
            this.nodeCount = nodeCount;
            this.relationshipCount = relationshipCount;
            this.projectMillis = projectMillis;
        }

        protected abstract static class Builder {
            final String graphName;
            long nodeCount;
            long relationshipCount;
            long projectMillis;

            Builder(GraphProjectConfig config) {
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

            Builder withProjectMillis(long projectMillis) {
                this.projectMillis = projectMillis;
                return this;
            }

            abstract GraphProjectResult build();
        }
    }

    @SuppressWarnings("unused")
    public static class GraphProjectNativeResult extends GraphProjectResult {

        public final Map<String, Object> nodeProjection;
        public final Map<String, Object> relationshipProjection;

        GraphProjectNativeResult(
            String graphName,
            Map<String, Object> nodeProjection,
            Map<String, Object> relationshipProjection,
            long nodeCount,
            long relationshipCount,
            long projectMillis
        ) {
            super(graphName, nodeCount, relationshipCount, projectMillis);
            this.nodeProjection = nodeProjection;
            this.relationshipProjection = relationshipProjection;
        }

        protected static final class Builder extends GraphProjectResult.Builder {
            private final NodeProjections nodeProjections;
            private final RelationshipProjections relationshipProjections;

            Builder(GraphProjectFromStoreConfig config) {
                super(config);
                this.nodeProjections = config.nodeProjections();
                this.relationshipProjections = config.relationshipProjections();
            }

            GraphProjectNativeResult build() {
                return new GraphProjectNativeResult(
                    graphName,
                    nodeProjections.toObject(),
                    relationshipProjections.toObject(),
                    nodeCount,
                    relationshipCount,
                    projectMillis
                );
            }
        }
    }

    @SuppressWarnings("unused")
    public static class GraphProjectCypherResult extends GraphProjectResult {
        public final String nodeQuery;
        public final String relationshipQuery;

        GraphProjectCypherResult(
            String graphName,
            String nodeQuery,
            String relationshipQuery,
            long nodeCount,
            long relationshipCount,
            long projectMillis
        ) {
            super(graphName, nodeCount, relationshipCount, projectMillis);
            this.nodeQuery = nodeQuery;
            this.relationshipQuery = relationshipQuery;
        }

        protected static final class Builder extends GraphProjectResult.Builder {
            private final String nodeQuery;
            private final String relationshipQuery;

            Builder(GraphProjectFromCypherConfig config) {
                super(config);
                this.nodeQuery = config.nodeQuery();
                this.relationshipQuery = config.relationshipQuery();
            }

            GraphProjectCypherResult build() {
                return new GraphProjectCypherResult(
                    graphName,
                    nodeQuery,
                    relationshipQuery,
                    nodeCount,
                    relationshipCount,
                    projectMillis
                );
            }
        }
    }

    @SuppressWarnings("unused")
    public static class GraphProjectSubgraphResult extends GraphProjectResult {
        public final String fromGraphName;
        public final String nodeFilter;
        public final String relationshipFilter;

        GraphProjectSubgraphResult(
            String graphName,
            String fromGraphName,
            String nodeFilter,
            String relationshipFilter,
            long nodeCount,
            long relationshipCount,
            long projectMillis
        ) {
            super(graphName, nodeCount, relationshipCount, projectMillis);
            this.fromGraphName = fromGraphName;
            this.nodeFilter = nodeFilter;
            this.relationshipFilter = relationshipFilter;
        }
    }
}
