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

import com.carrotsearch.hppc.IntArrayDeque;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.ShortestPathAStar;
import org.neo4j.graphalgo.impl.ShortestPathDijkstra;
import org.neo4j.graphalgo.results.DijkstraResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ShortestPathProc {

    public static final String DEFAULT_TARGET_PROPERTY = "sssp";


    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    /**
     * single threaded dijkstra impl.
     * takes a startNode and endNode id and tries to find the best path
     * supports direction flag in configuration ( see {@link org.neo4j.graphalgo.core.utils.Directions})
     * default is: BOTH
     *
     * @param startNode
     * @param endNode
     * @param propertyName
     * @param config
     * @return
     */
    @Procedure("algo.shortestPath.stream")
    @Description("CALL algo.shortestPath.stream(startNode:Node, endNode:Node, weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', direction:'BOTH', defaultValue:1.0}) " +
            "YIELD nodeId, cost - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPathDijkstra.Result> dijkstraStream(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name(value = "propertyName", defaultValue = "") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Direction direction = configuration.getDirection(Direction.BOTH);

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                .withOptionalRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getWeightPropertyDefaultValue(1.0));

        if(direction == Direction.BOTH) {
            direction = Direction.OUTGOING;
            graphLoader.asUndirected(true).withDirection(direction);
        } else {
            graphLoader.withDirection(direction);
        }


        final Graph graph = graphLoader.load(configuration.getGraphImpl());

        if (graph.isEmpty() || startNode == null || endNode == null) {
            graph.release();
            return Stream.empty();
        }

        return new ShortestPathDijkstra(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(Dijkstra)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute(startNode.getId(), endNode.getId(), direction)
                .resultStream();
    }

    @Procedure(value = "algo.shortestPath", mode = Mode.WRITE)
    @Description("CALL algo.shortestPath(startNode:Node, endNode:Node, weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', direction:'BOTH', defaultValue:1.0, write:'true', writeProperty:'sssp'}) " +
            "YIELD nodeId, cost, loadMillis, evalMillis, writeMillis - yields nodeCount, totalCost, loadMillis, evalMillis, writeMillis")
    public Stream<DijkstraResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name(value = "propertyName", defaultValue = "") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        DijkstraResult.Builder builder = DijkstraResult.builder();

        final Graph graph;
        final ShortestPathDijkstra dijkstra;

        Direction direction = configuration.getDirection(Direction.BOTH);
        try (ProgressTimer timer = builder.timeLoad()) {
            GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                    .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                    .withOptionalRelationshipWeightsFromProperty(
                            propertyName,
                            configuration.getWeightPropertyDefaultValue(1.0))
                    .withDirection(direction);

            if(direction == Direction.BOTH) {
                direction = Direction.OUTGOING;
                graphLoader.asUndirected(true).withDirection(direction);
            } else {
                graphLoader.withDirection(direction);
            }


            graph = graphLoader.load(configuration.getGraphImpl());
        }

        if (graph.isEmpty() || startNode == null || endNode == null) {
            graph.release();
            return Stream.of(builder.build());
        }

        try (ProgressTimer timer = builder.timeEval()) {
            dijkstra = new ShortestPathDijkstra(graph)
                    .withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(Dijkstra)"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .compute(startNode.getId(), endNode.getId(), direction);
            builder.withNodeCount(dijkstra.getPathLength())
                    .withTotalCosts(dijkstra.getTotalCost());
        }

        if (configuration.isWriteFlag()) {
            try (ProgressTimer timer = builder.timeWrite()) {
                final IntArrayDeque finalPath = dijkstra.getFinalPath();
                dijkstra.release();

                final DequeMapping mapping = new DequeMapping(graph, finalPath);
                Exporter.of(api, mapping)
                        .withLog(log)
                        .build()
                        .write(
                                configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY),
                                finalPath,
                                (PropertyTranslator.OfInt<IntArrayDeque>) (data, nodeId) -> (int) nodeId
                        );
            }
        }

        return Stream.of(builder.build());
    }

    @Procedure("algo.shortestPath.astar.stream")
    @Description("CALL algo.shortestPath.astar.stream(startNode:Node, endNode:Node, weightProperty:String, propertyKeyLat:String," +
    		"propertyKeyLon:String, {nodeQuery:'labelName', relationshipQuery:'relationshipName', direction:'BOTH', defaultValue:1.0}) " +
    		"YIELD nodeId, cost - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPathAStar.Result> astarStream(
    			@Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("propertyName") String propertyName,
            @Name(value = "propertyKeyLat", defaultValue = "latitude") String propertyKeyLat,
            @Name(value = "propertyKeyLon", defaultValue = "longitude") String propertyKeyLon,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Direction direction = configuration.getDirection(Direction.BOTH);

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                .withOptionalRelationshipWeightsFromProperty(propertyName, configuration.getWeightPropertyDefaultValue(1.0))
                .withDirection(direction);


        if(direction == Direction.BOTH) {
            direction = Direction.OUTGOING;
            graphLoader.asUndirected(true).withDirection(direction);
        } else {
            graphLoader.withDirection(direction);
        }

        final Graph graph = graphLoader.load(configuration.getGraphImpl());

            if (graph.isEmpty() || startNode == null || endNode == null) {
                graph.release();
                return Stream.empty();
            }

    		return new ShortestPathAStar(graph, api)
    				.withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(AStar)"))
    				.withTerminationFlag(TerminationFlag.wrap(transaction))
    				.compute(startNode.getId(), endNode.getId(), propertyKeyLat, propertyKeyLon, direction)
    				.resultStream();
    }

    private static final class DequeMapping implements IdMapping {
        private final IdMapping mapping;
        private final int[] data;
        private final int offset;
        private final int length;

        private DequeMapping(IdMapping mapping, IntArrayDeque data) {
            this.mapping = mapping;
            if (data.head <= data.tail) {
                this.data = data.buffer;
                this.offset = data.head;
                this.length = data.tail - data.head;
            } else {
                this.data = data.toArray();
                this.offset = 0;
                this.length = this.data.length;
            }
        }

        @Override
        public long toMappedNodeId(final long nodeId) {
            return mapping.toMappedNodeId(nodeId);
        }

        @Override
        public long toOriginalNodeId(final long nodeId) {
            assert nodeId < length;
            return mapping.toOriginalNodeId(data[offset + Math.toIntExact(nodeId)]);
        }

        @Override
        public boolean contains(final long nodeId) {
            return true;
        }

        @Override
        public long nodeCount() {
            return length;
        }
    }

}
