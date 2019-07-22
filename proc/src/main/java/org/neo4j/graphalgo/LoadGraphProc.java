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
package org.neo4j.graphalgo;

import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.loading.LoadGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public final class LoadGraphProc extends BaseProc {
    @Procedure(name = "algo.graph.load")
    @Description("CALL algo.graph.load(" +
            "name:String, label:String, relationship:String" +
            "{direction:'OUT/IN/BOTH', undirected:true/false, sorted:true/false, nodeProperty:'value', nodeWeight:'weight', relationshipWeight: 'weight', graph:'heavy/huge/cypher'}) " +
            "YIELD nodes, relationships, loadMillis, computeMillis, writeMillis, write, nodeProperty, nodeWeight, relationshipWeight - " +
            "load named graph")
    public Stream<LoadGraphStats> load(
            @Name(value = "name", defaultValue = "") String name,
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationshipType);

        LoadGraphStats stats = new LoadGraphStats(name, configuration);

        if (LoadGraphFactory.check(name)) {
            // return already loaded
            stats.alreadyLoaded = true;
            return Stream.of(stats);
        }

        try (ProgressTimer timer = ProgressTimer.start()) {
            Class<? extends GraphFactory> graphImpl = configuration.getGraphImpl();
            GraphLoader loader = newLoader(configuration, AllocationTracker.EMPTY);
            Graph graph = runWithExceptionLogging("Graph loading failed", () -> loader.load(graphImpl));

            stats.nodes = graph.nodeCount();
            stats.relationships = graph.relationshipCount();
            stats.loadMillis = timer.stop().getDuration();
            LoadGraphFactory.set(name, graph);
        }

        return Stream.of(stats);
    }

    @Procedure(name = "algo.graph.load.memrec")
    @Description("CALL algo.graph.load.memrec(" +
                 "label:String, relationship:String" +
                 "{direction:'OUT/IN/BOTH', undirected:true/false, sorted:true/false, nodeProperty:'value', nodeWeight:'weight', relationshipWeight: 'weight', graph:'heavy/huge'}) " +
                 "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for the graph")
    public Stream<MemRecResult> loadMemRec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration) {
        ProcedureConfiguration config = newConfig(label, relationshipType, configuration);
        GraphLoader loader = newLoader(config, AllocationTracker.EMPTY);
        GraphFactory graphFactory = loader.build(config.getGraphImpl());
        MemoryTree memoryTree = graphFactory.memoryEstimation().estimate(graphFactory.dimensions(), config.getConcurrency());
        return Stream.of(new MemRecResult(new MemoryTreeWithDimensions(memoryTree, graphFactory.dimensions())));
    }

    @Override
    GraphLoader configureLoader(final GraphLoader loader, final ProcedureConfiguration config) {
        final Direction direction = config.getDirection(Direction.OUTGOING);
        final String nodeWeight = config.getString("nodeWeight", null);
        final String nodeProperty = config.getString("nodeProperty", null);
        final Boolean sorted = config.get("sorted", false);
        final Boolean undirected = config.get("undirected", false);
        return loader
                .withNodeStatement(config.getNodeLabelOrQuery())
                .withRelationshipStatement(config.getRelationshipOrQuery())
                .withOptionalRelationshipWeightsFromProperty(
                        config.getWeightProperty(),
                        config.getWeightPropertyDefaultValue(1.0))
                .withOptionalNodeProperty(nodeProperty, 0.0d)
                .withOptionalNodeWeightsFromProperty(nodeWeight, 1.0d)
                .withOptionalNodeProperties(
                        PropertyMapping.of(LabelPropagation.SEED_TYPE, nodeProperty, 0.0d),
                        PropertyMapping.of(LabelPropagation.WEIGHT_TYPE, nodeWeight, 1.0d)
                )
                .withDirection(direction)
                .withSort(sorted)
                .asUndirected(undirected);
    }

    public static class LoadGraphStats {
        public String name, graph, direction;
        public boolean undirected;
        public boolean sorted;
        public long nodes, relationships, loadMillis;
        public boolean alreadyLoaded;
        public String nodeWeight, relationshipWeight, nodeProperty, loadNodes, loadRelationships;

        LoadGraphStats(String graphName, ProcedureConfiguration configuration) {
            name = graphName;
            graph = configuration.getString(ProcedureConstants.GRAPH_IMPL_PARAM, "heavy");
            undirected = configuration.get(ProcedureConstants.UNDIRECTED, false);
            sorted = configuration.get(ProcedureConstants.SORTED, false);
            loadNodes = configuration.getNodeLabelOrQuery();
            loadRelationships = configuration.getRelationshipOrQuery();
            direction = configuration.getDirection(Direction.OUTGOING).name();
            nodeWeight = configuration.getString(ProcedureConstants.NODE_WEIGHT, null);
            nodeProperty = configuration.getString(ProcedureConstants.NODE_PROPERTY, null);;
            relationshipWeight = configuration.getString(ProcedureConstants.RELATIONSHIP_WEIGHT, null);;
        }
    }

    @Procedure(name = "algo.graph.remove")
    @Description("CALL algo.graph.remove(name:String)")
    public Stream<GraphInfo> remove(@Name("name") String name) {
        GraphInfo info = new GraphInfo(name);

        Graph graph = LoadGraphFactory.get(name);
        if (graph != null) {
            info.type = graph.getType();
            info.nodes = graph.nodeCount();
            info.relationships = graph.relationshipCount();
            info.exists = LoadGraphFactory.remove(name);
            info.removed = true;
        }
        return Stream.of(info);
    }

    @Procedure(name = "algo.graph.info")
    @Description("CALL algo.graph.info(name:String, " +
            "degreeDistribution:bool | { direction:'OUT/IN/BOTH', concurrency:int }) " +
            "YIELD name, type, exists, nodes, relationships")
    public Stream<GraphInfoWithHistogram> info(
            @Name("name") String name,
            @Name(value = "degreeDistribution", defaultValue = "null") Object degreeDistribution) {
        Graph graph = LoadGraphFactory.get(name);
        final GraphInfoWithHistogram info;
        if (graph == null) {
            info = new GraphInfoWithHistogram(name);
        } else {
            final boolean calculateDegreeDistribution;
            final ProcedureConfiguration configuration;
            if (Boolean.TRUE.equals(degreeDistribution)) {
                calculateDegreeDistribution = true;
                configuration = ProcedureConfiguration.empty();
            } else if (degreeDistribution instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> config = (Map) degreeDistribution;
                calculateDegreeDistribution = !config.isEmpty();
                configuration = ProcedureConfiguration.create(config);
            } else {
                calculateDegreeDistribution = false;
                configuration = ProcedureConfiguration.empty();
            }

            if (calculateDegreeDistribution) {
                final Direction direction = configuration.getDirection(Direction.OUTGOING);
                int concurrency = configuration.getReadConcurrency();
                Histogram distribution = degreeDistribution(graph, concurrency, direction);
                info = new GraphInfoWithHistogram(name, distribution);
            } else {
                info = new GraphInfoWithHistogram(name);
            }
            info.type = graph.getType();
            info.nodes = graph.nodeCount();
            info.relationships = graph.relationshipCount();
            info.exists = true;
        }
        return Stream.of(info);
    }

    private Histogram degreeDistribution(Graph graph, final int concurrency, final Direction direction) {
        int batchSize = Math.toIntExact(ParallelUtil.adjustBatchSize(
                graph.nodeCount(),
                concurrency,
                ParallelUtil.DEFAULT_BATCH_SIZE));
        AtomicHistogram histogram = new AtomicHistogram(graph.relationshipCount(), 3);

        ParallelUtil.readParallel(
                concurrency,
                batchSize,
                graph,
                Pools.DEFAULT,
                (nodeOffset, nodeIds) -> () -> {
                    PrimitiveLongIterator iterator = nodeIds.iterator();
                    while (iterator.hasNext()) {
                        long nodeId = iterator.next();
                        int degree = graph.degree(nodeId, direction);
                        histogram.recordValue(degree);
                    }
                }
        );
        return histogram;
    }

    public static class GraphInfo {
        public final String name;
        public String type;
        public boolean exists;
        public boolean removed;
        public long nodes, relationships;

        public GraphInfo(String name) {
            this.name = name;
        }
    }

    public static class GraphInfoWithHistogram {
        public final String name;
        public String type;
        public boolean exists;
        public long nodes, relationships;

        public long max, min;
        public double mean;
        public long p50, p75, p90, p95, p99, p999;

        public GraphInfoWithHistogram(String name) {
            this.name = name;
        }

        public GraphInfoWithHistogram(String name, Histogram histogram) {
            this(name);
            this.max = histogram.getMaxValue();
            this.min = histogram.getMinValue();
            this.mean = histogram.getMean();
            this.p50 = histogram.getValueAtPercentile(50);
            this.p75 = histogram.getValueAtPercentile(75);
            this.p90 = histogram.getValueAtPercentile(90);
            this.p95 = histogram.getValueAtPercentile(95);
            this.p99 = histogram.getValueAtPercentile(99);
            this.p999 = histogram.getValueAtPercentile(99.9);
        }
    }
}
