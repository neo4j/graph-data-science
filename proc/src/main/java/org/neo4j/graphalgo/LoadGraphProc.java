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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public final class LoadGraphProc {

    @Context
    public GraphDatabaseAPI dbAPI;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

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
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationshipType);

        final Direction direction = configuration.getDirection(Direction.OUTGOING);
        final String relationshipWeight = configuration.getString("relationshipWeight", null);
        final String nodeWeight = configuration.getString("nodeWeight", null);
        final String nodeProperty = configuration.getString("nodeProperty", null);

        LoadGraphStats stats = new LoadGraphStats();
        stats.name = name;
        stats.graph = configuration.getString(ProcedureConstants.GRAPH_IMPL_PARAM, "heavy");
        stats.undirected = configuration.get("undirected", false);
        stats.sorted = configuration.get("sorted", false);
        stats.loadNodes = label;
        stats.loadRelationships = relationshipType;
        stats.direction = direction.name();
        stats.nodeWeight = nodeWeight;
        stats.nodeProperty = nodeProperty;
        stats.relationshipWeight = relationshipWeight;

        if (LoadGraphFactory.check(name)) {
            // return already loaded
            stats.alreadyLoaded = true;
            return Stream.of(stats);
        }

        try (ProgressTimer timer = ProgressTimer.start()) {
            Class<? extends GraphFactory> graphImpl = configuration.getGraphImpl();

            Graph graph = new GraphLoader(dbAPI, Pools.DEFAULT)
                    .init(log, configuration.getNodeLabelOrQuery(),
                            configuration.getRelationshipOrQuery(), configuration)
                    .withName(name)
                    .withAllocationTracker(new AllocationTracker())
                    .withOptionalRelationshipWeightsFromProperty(relationshipWeight, 1.0d)
                    .withOptionalNodeProperty(nodeProperty, 0.0d)
                    .withOptionalNodeWeightsFromProperty(nodeWeight, 1.0d)
                    .withOptionalNodeProperties(
                            PropertyMapping.of(LabelPropagation.PARTITION_TYPE, nodeProperty, 0.0d),
                            PropertyMapping.of(LabelPropagation.WEIGHT_TYPE, nodeWeight, 1.0d)
                    )
                    .withDirection(direction)
                    .withSort(stats.sorted)
                    .asUndirected(stats.undirected)
                    .load(graphImpl);

            stats.nodes = graph.nodeCount();
            stats.relationships = graph.relationshipCount();
            stats.loadMillis = timer.stop().getDuration();
            LoadGraphFactory.set(name, graph);
        }

        return Stream.of(stats);
    }

    public static class LoadGraphStats {
        public String name, graph, direction;
        public boolean undirected;
        public boolean sorted;
        public long nodes, relationships, loadMillis;
        public boolean alreadyLoaded;
        public String nodeWeight, relationshipWeight, nodeProperty, loadNodes, loadRelationships;
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
    public Stream<GraphInfo> info(
            @Name("name") String name,
            @Name(value = "degreeDistribution", defaultValue = "null") Object degreeDistribution) {
        Graph graph = LoadGraphFactory.get(name);
        final GraphInfo info;
        if (graph != null) {

            final boolean calculatedegreeDistribution;
            final ProcedureConfiguration configuration;
            if (Boolean.TRUE.equals(degreeDistribution)) {
                calculatedegreeDistribution = true;
                configuration = ProcedureConfiguration.create(Collections.emptyMap());
            } else if (degreeDistribution instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> config = (Map) degreeDistribution;
                calculatedegreeDistribution = !config.isEmpty();
                configuration = ProcedureConfiguration.create(config);
            } else {
                calculatedegreeDistribution = false;
                configuration = null;
            }

            if (calculatedegreeDistribution) {
                final Direction direction = configuration.getDirection(Direction.OUTGOING);
                int concurrency = configuration.getReadConcurrency();
                Histogram distribution = degreeDistribution(graph, concurrency, direction);
                info = new GraphInfo(name, distribution);
            } else {
                info = new GraphInfo(name);
            }
            info.type = graph.getType();
            info.nodes = graph.nodeCount();
            info.relationships = graph.relationshipCount();
            info.exists = true;
        } else {
            info = new GraphInfo(name);
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
        public long p50, p75, p90, p95, p99, p999, max, min;
        public double mean;

        public GraphInfo(String name) {
            this.name = name;
        }

        public GraphInfo(String name, Histogram histogram) {
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
