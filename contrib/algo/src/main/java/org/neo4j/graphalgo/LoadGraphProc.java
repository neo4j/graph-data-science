/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.loading.LoadGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.LabelPropagationAlgorithm;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

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
        stats.graph = configuration.getString(ProcedureConstants.GRAPH_IMPL_PARAM,"heavy");
        stats.undirected = configuration.get("undirected",false);
        stats.sorted = configuration.get("sorted",false);
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
                            PropertyMapping.of(LabelPropagationAlgorithm.PARTITION_TYPE, nodeProperty, 0.0d),
                            PropertyMapping.of(LabelPropagationAlgorithm.WEIGHT_TYPE, nodeWeight, 1.0d)
                    )
                    .withDirection(direction)
                    .withSort(stats.sorted)
                    .asUndirected(stats.undirected)
                    .load(graphImpl);
            stats.nodes=graph.nodeCount();
            stats.loadMillis = timer.stop().getDuration();
            LoadGraphFactory.set(name, graph);
        }

        return Stream.of(stats);
    }

    public static class LoadGraphStats {
        public String name, graph, direction;
        public boolean undirected;
        public boolean sorted;
        public long nodes, loadMillis;
        public boolean alreadyLoaded;
        public String nodeWeight, relationshipWeight, nodeProperty, loadNodes, loadRelationships;
    }

    @Procedure(name = "algo.graph.remove")
    @Description("CALL algo.graph.remove(name:String")
    public Stream<GraphInfo> remove(@Name("name") String name) {
        GraphInfo info = new GraphInfo(name);

        Graph graph = LoadGraphFactory.get(name);
        if (graph != null) {
            info.type = graph.getType();
            info.nodes = graph.nodeCount();
            info.exists = LoadGraphFactory.remove(name);
            info.removed = true;
        }
        return Stream.of(info);
    }

    @Procedure(name = "algo.graph.info")
    @Description("CALL algo.graph.info(name:String")
    public Stream<GraphInfo> info(@Name("name") String name) {
        GraphInfo info = new GraphInfo(name);
        Graph graph = LoadGraphFactory.get(name);
        if (graph != null) {
            info.type = graph.getType();
            info.nodes = graph.nodeCount();
            info.exists = true;
        }
        return Stream.of(info);
    }

    public static class GraphInfo {
        public final String name;
        public String type;
        public boolean exists;
        public boolean removed;
        public long nodes;

        public GraphInfo(String name) {
            this.name = name;
        }
    }
}
