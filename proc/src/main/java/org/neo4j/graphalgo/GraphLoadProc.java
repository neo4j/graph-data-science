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
package org.neo4j.graphalgo;

import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.ProjectionParser;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public final class GraphLoadProc extends BaseProc {
    @Procedure(name = "algo.graph.load", mode = Mode.READ)
    @Description("CALL algo.graph.load(" +
                 "name:String, label:String, relationship:String" +
                 "{direction:'OUT/IN/BOTH', undirected:true/false, sorted:true/false, nodeProperty:'value', nodeWeight:'weight', relationshipWeight: 'weight', relationshipProperties: {}, graph:'huge/cypher'}) " +
                 "YIELD nodes, relationships, loadMillis, computeMillis, writeMillis, write, nodeProperty, nodeWeight, relationshipWeight, relationshipProperties - " +
                 "load named graph")
    public Stream<GraphLoadStats> load(
            @Name(value = "name", defaultValue = "") String name,
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration
            .create(config, getUsername())
            .setNodeLabelOrQuery(label)
            .setRelationshipTypeOrQuery(relationshipType);

        GraphLoadStats stats = runWithExceptionLogging(
            "Graph loading failed",
            () -> this.loadGraph(configuration, name)
        );
        return Stream.of(stats);
    }

    @Procedure(name = "algo.graph.load.memrec")
    @Description("CALL algo.graph.load.memrec(" +
                 "label:String, relationship:String" +
                 "{direction:'OUT/IN/BOTH', undirected:true/false, sorted:true/false, nodeProperty:'value', nodeWeight:'weight', relationshipWeight: 'weight', relationshipProperties: {}, graph:'cypher/huge'}) " +
                 "YIELD requiredMemory, treeView, bytesMin, bytesMax - estimates memory requirements for the graph")
    public Stream<MemoryEstimateResult> loadMemRec(
        @Name(value = "label", defaultValue = "") String label,
        @Name(value = "relationship", defaultValue = "") String relationshipType,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> configuration) {

        ProcedureConfiguration config = ProcedureConfiguration
            .create(configuration, getUsername())
            .setNodeLabelOrQuery(label)
            .setRelationshipTypeOrQuery(relationshipType);

        GraphLoader loader = getLoader(config);
        GraphFactory graphFactory = loader.build(config.getGraphImpl());
        GraphDimensions dimensions = graphFactory.dimensions();;

        MemoryTree memoryTree = config
            .estimate(loader.toSetup(), graphFactory)
            .estimate(dimensions, config.concurrency());

        return Stream.of(new MemoryEstimateResult(new MemoryTreeWithDimensions(memoryTree, dimensions)));
    }

    private GraphLoadStats loadGraph(ProcedureConfiguration config, String graphName) {
        GraphLoadStats stats = new GraphLoadStats(graphName, config);

        if (GraphCatalog.exists(getUsername(), graphName)) {
            throw new IllegalArgumentException(String.format("A graph with name '%s' is already loaded.", graphName));
        }

        try (ProgressTimer ignored = ProgressTimer.start(time -> stats.loadMillis = time)) {
            Class<? extends GraphFactory> graphImpl = config.getGraphImpl();
            boolean isCypher = graphImpl == CypherGraphFactory.class;
            Set<String> relationshipTypes = isCypher
                    ? Collections.emptySet()
                    : ProjectionParser.parse(config.getRelationshipOrQuery());

            PropertyMappings propertyMappings = config.getRelationshipProperties();

            GraphLoader loader = getLoader(config);

            GraphsByRelationshipType graph;
            if (!relationshipTypes.isEmpty() || propertyMappings.hasMappings()) {
                graph = loader.build(graphImpl).build().graphs();
            } else {
                graph = GraphsByRelationshipType.of(loader.load(graphImpl));
            }

            stats.nodes = graph.nodeCount();
            stats.relationships = graph.relationshipCount();

            GraphCreateConfig createConfig = isCypher
                ? GraphCreateFromCypherConfig.emptyWithName(config.getUsername(), graphName)
                : GraphCreateFromStoreConfig.emptyWithName(config.getUsername(), graphName);

            GraphCatalog.set(createConfig, graph);
        }

        return stats;
    }

    private GraphLoader getLoader(ProcedureConfiguration config) {
        GraphLoader loader = new GraphLoader(api, Pools.DEFAULT)
            .init(log, getUsername())
            .withAllocationTracker(AllocationTracker.EMPTY)
            .withTerminationFlag(TerminationFlag.wrap(transaction));

        final Direction direction = config.getDirection(Direction.OUTGOING);
        loader
            .withNodeStatement(config.getNodeLabelOrQuery())
            .withRelationshipStatement(config.getRelationshipOrQuery())
            .withOptionalNodeProperties(config.getNodeProperties())
            .withRelationshipProperties(config.getRelationshipProperties())
            .withDirection(direction);

        if (config.containsKey(ProcedureConstants.RELATIONSHIP_WEIGHT_KEY)) { // required to be backwards compatible with `relationshipWeight`
            loader.withRelationshipProperties(PropertyMapping.of(
                config.getString(ProcedureConstants.RELATIONSHIP_WEIGHT_KEY, null),
                config.getWeightPropertyDefaultValue(ProcedureConstants.DEFAULT_VALUE_DEFAULT)
            ));
        } else if (config.hasWeightProperty()) { // required to be backwards compatible with `weightProperty` (not documented but was possible)
            loader.withRelationshipProperties(PropertyMapping.of(
                config.getWeightProperty(),
                config.getWeightPropertyDefaultValue(ProcedureConstants.DEFAULT_VALUE_DEFAULT)));
        }

        if (config.get(ProcedureConstants.SORTED_KEY, false)) {
            loader.sorted();
        }
        if (config.get(ProcedureConstants.UNDIRECTED_KEY, false)) {
            loader.undirected();
        }
        return loader;
    }

    public static class GraphLoadStats {
        public String name, graph, direction;
        public boolean undirected;
        public boolean sorted;
        public long nodes, relationships, loadMillis;
        public final boolean alreadyLoaded = false; // No longer used--we throw an exception instead.
        public String loadNodes;
        public String loadRelationships;
        public Object nodeProperties;
        public Object relationshipProperties;
        public String relationshipWeight;

        GraphLoadStats(String graphName, ProcedureConfiguration configuration) {
            name = graphName;
            graph = configuration.getString(ProcedureConstants.GRAPH_IMPL_KEY, "huge");
            undirected = configuration.get(ProcedureConstants.UNDIRECTED_KEY, false);
            sorted = configuration.get(ProcedureConstants.SORTED_KEY, false);
            loadNodes = configuration.getNodeLabelOrQuery();
            loadRelationships = configuration.getRelationshipOrQuery();
            direction = configuration.getDirection(Direction.OUTGOING).name();
            nodeProperties = configuration.get(ProcedureConstants.NODE_PROPERTIES_KEY, null);
            // default is required to be backwards compatible with `weightProperty` (not documented but was possible)
            relationshipWeight = configuration.getString(ProcedureConstants.RELATIONSHIP_WEIGHT_KEY, configuration.getWeightProperty());
            relationshipProperties = configuration.get(ProcedureConstants.RELATIONSHIP_PROPERTIES_KEY, null);
        }
    }

    @Procedure(name = "algo.graph.remove", mode = Mode.WRITE)
    @Description("CALL algo.graph.remove(name:String)")
    public Stream<GraphInfo> remove(@Name("name") String name) {
        GraphInfo info = new GraphInfo(name);

        Graph graph = GraphCatalog.remove(getUsername(), name);
        if (graph != null) {
            info.type = graph.getType();
            info.nodes = graph.nodeCount();
            info.relationships = graph.relationshipCount();
            info.exists = true;
            info.removed = true;
        }
        return Stream.of(info);
    }

    @Procedure(name = "algo.graph.info")
    @Description("CALL algo.graph.info(name:String, " +
                 "degreeDistribution:bool | { direction:'OUT/IN/BOTH', concurrency:int }) " +
                 "YIELD name, type, direction, exists, nodes, relationships")
    public Stream<GraphInfoWithHistogram> info(
            @Name("name") String name,
            @Name(value = "degreeDistribution", defaultValue = "null") Object degreeDistribution) {
        final GraphInfoWithHistogram info;
        Optional<Graph> maybeGraph = GraphCatalog.getUnion(getUsername(), name);
        if (!maybeGraph.isPresent()) {
            info = new GraphInfoWithHistogram(name);
        } else {
            Graph graph = maybeGraph.get();
            final boolean calculateDegreeDistribution;
            final ProcedureConfiguration configuration;
            if (Boolean.TRUE.equals(degreeDistribution)) {
                calculateDegreeDistribution = true;
                configuration = ProcedureConfiguration.create(getUsername());
            } else if (degreeDistribution instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> config = (Map) degreeDistribution;
                calculateDegreeDistribution = !config.isEmpty();
                configuration = ProcedureConfiguration.create(config, getUsername());
            } else {
                calculateDegreeDistribution = false;
                configuration = ProcedureConfiguration.create(getUsername());
            }

            if (calculateDegreeDistribution) {
                final Direction direction = configuration.getDirection(graph.getLoadDirection());
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
            info.direction = graph.getLoadDirection().name();
        }
        return Stream.of(info);
    }

    @Procedure(name = "algo.graph.list")
    @Description("CALL algo.graph.list() " +
                 "YIELD name, type, nodes, relationships, direction" +
                 "list all loaded graphs")
    public Stream<GraphInfo> list() {
        Map<String, Graph> loadedGraphs = GraphCatalog.getLoadedGraphsByName(getUsername());

        return loadedGraphs.entrySet().stream().map(entry -> {
            Graph graph = entry.getValue();
            return new GraphInfo(
                    entry.getKey(),
                    graph.getType(),
                    graph.nodeCount(),
                    graph.relationshipCount(),
                    graph.getLoadDirection().toString());
        });
    }

    private Histogram degreeDistribution(Graph graph, final int concurrency, final Direction direction) {
        int batchSize = Math.toIntExact(ParallelUtil.adjustedBatchSize(
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
        public String direction;

        public GraphInfo(String name) {
            this.name = name;
        }

        public GraphInfo(
                String name,
                String type,
                long nodes,
                long relationships,
                String direction) {
            this.name = name;
            this.type = type;
            this.nodes = nodes;
            this.relationships = relationships;
            this.direction = direction;
            this.exists = true;
            this.removed = false;
        }
    }

    public static class GraphInfoWithHistogram {
        public final String name;
        public String type;
        public boolean exists;
        public long nodes, relationships;
        public String direction;

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
