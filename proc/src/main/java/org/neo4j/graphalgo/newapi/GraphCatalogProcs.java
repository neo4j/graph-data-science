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

import org.HdrHistogram.AtomicHistogram;
import org.jetbrains.annotations.Nullable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class GraphCatalogProcs extends BaseProc<GraphCreateConfig> {

    private static final String HISTOGRAM_FIELD_NAME = "histogram";

    @Override
    protected GraphLoader newConfigureLoader(
        GraphLoader loader,
        GraphCreateConfig config
    ) {
        return loader;
    }

    @Procedure(name = "algo.beta.graph.create", mode = Mode.READ)
    @Description("CALL graph.create(" +
                 "  graphName: STRING," +
                 "  nodeFilter: MAP," +
                 "  relationshipFilter: MAP," +
                 "  configuration: MAP" +
                 ") YIELD" +
                 "  graphName: STRING," +
                 "  nodeFilter: MAP," +
                 "  relationshipFilter: MAP," +
                 "  nodes: INTEGER," +
                 "  relationships: INTEGER," +
                 "  createMillis: INTEGER")
    public Stream<GraphCreateResult> create(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeFilter") @Nullable Object nodeFilter,
        @Name(value = "relationshipFilter") @Nullable Object relationshipFilter,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        // input
        GraphCreateConfig config = GraphCreateConfig.of(
            getUsername(),
            graphName,
            nodeFilter,
            relationshipFilter,
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
            throw new IllegalArgumentException(String.format("A graph with name '%s' already exists.", config.graphName()));
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

    @Procedure(name = "algo.beta.graph.list", mode = Mode.READ)
    public Stream<GraphInfo> list(@Name(value = "graphName", defaultValue = "") String graphName) {
        CypherMapWrapper.failOnNull("graphName", graphName);

        boolean computeHistogram = callContext.outputFields().anyMatch(HISTOGRAM_FIELD_NAME::equals);
        Stream<Map.Entry<GraphCreateConfig, Graph>> graphEntries;

        graphEntries = GraphCatalog.getLoadedGraphs(getUsername()).entrySet().stream();
        if (!isEmpty(graphName)) {
            // we should only list the provided graph
            graphEntries = graphEntries.filter(e -> e.getKey().graphName().equals(graphName));
        }

        return graphEntries.map(e -> new GraphInfo(e.getKey(), e.getValue(), computeHistogram));
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

    public static class GraphInfo {

        public final String graphName;
        public final Map<String, Object> nodeProjection, relationshipProjection;
        public final long nodes, relationships;
        public final Map<String, Object> histogram;

        GraphInfo(GraphCreateConfig config, Graph graph, boolean computeHistogram) {
            this.graphName = config.graphName();
            nodeProjection = config.nodeProjection().toObject();
            relationshipProjection = config.relationshipProjection().toObject();
            nodes = graph.nodeCount();
            relationships = graph.relationshipCount();
            histogram = computeHistogram ? computeHistogram(graph) : emptyMap();
        }

        private static final int PRECISION = 5;

        private Map<String, Object> computeHistogram(Graph graph) {
            int batchSize = Math.toIntExact(ParallelUtil.adjustedBatchSize(
                graph.nodeCount(),
                Pools.DEFAULT_CONCURRENCY,
                ParallelUtil.DEFAULT_BATCH_SIZE
            ));
            // needs to be at least 2 due to some requirement from the AtomicHistogram, see their JavaDoc
            long maximumDegree = Math.max(2, graph.relationshipCount());
            AtomicHistogram histogram = new AtomicHistogram(maximumDegree, PRECISION);

            ParallelUtil.readParallel(
                Pools.DEFAULT_CONCURRENCY,
                batchSize,
                graph,
                Pools.DEFAULT,
                (nodeOffset, nodeIds) -> () -> {
                    PrimitiveLongIterator iterator = nodeIds.iterator();
                    while (iterator.hasNext()) {
                        long nodeId = iterator.next();
                        int degree = graph.degree(nodeId, Direction.OUTGOING);
                        histogram.recordValue(degree);
                    }
                }
            );
            return MapUtil.map(
                "min", histogram.getMinValue(),
                "mean", histogram.getMean(),
                "max", histogram.getMaxValue(),
                "p50", histogram.getValueAtPercentile(50),
                "p75", histogram.getValueAtPercentile(75),
                "p90", histogram.getValueAtPercentile(90),
                "p95", histogram.getValueAtPercentile(95),
                "p99", histogram.getValueAtPercentile(99),
                "p999", histogram.getValueAtPercentile(99.9)
            );
        }
    }
}
