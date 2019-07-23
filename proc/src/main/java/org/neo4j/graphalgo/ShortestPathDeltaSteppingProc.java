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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.ShortestPathDeltaStepping;
import org.neo4j.graphalgo.results.DeltaSteppingProcResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Delta-Stepping is a non-negative single source shortest paths (NSSSP) algorithm
 * to calculate the length of the shortest paths from a starting node to all other
 * nodes in the graph. It can be tweaked using the delta-parameter which controls
 * the grade of concurrency.<br>
 * <p>
 * More information in:<br>
 * <p>
 * <a href="https://arxiv.org/pdf/1604.02113v1.pdf">https://arxiv.org/pdf/1604.02113v1.pdf</a><br>
 * <a href="https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf">https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf</a><br>
 * <a href="http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf">http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf</a><br>
 * <a href="http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf">http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf</a>
 *
 * @author mknblch
 */
public class ShortestPathDeltaSteppingProc {

    public static final String WRITE_PROPERTY = "writeProperty";
    public static final String DEFAULT_TARGET_PROPERTY = "sssp";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure("algo.shortestPath.deltaStepping.stream")
    @Description("CALL algo.shortestPath.deltaStepping.stream(startNode:Node, weightProperty:String, delta:Double" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0, concurrency:4}) " +
            "YIELD nodeId, distance - yields a stream of {nodeId, distance} from start to end (inclusive)")
    public Stream<ShortestPathDeltaStepping.DeltaSteppingResult> deltaSteppingStream(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name("delta") Double delta,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Direction direction = configuration.getDirection(Direction.BOTH);

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                .withRelationshipWeightsFromProperty(
                        propertyName,
                        configuration.getWeightPropertyDefaultValue(Double.MAX_VALUE));

        if(direction == Direction.BOTH) {
            direction = Direction.OUTGOING;
            graphLoader.loadAsUndirected(true).withDirection(direction);
        } else {
            graphLoader.withDirection(direction);
        }

        final Graph graph = graphLoader.load(configuration.getGraphImpl());

        if (graph.isEmpty() || startNode == null) {
            graph.release();
            return Stream.empty();
        }

        final ShortestPathDeltaStepping algo = new ShortestPathDeltaStepping(graph, delta, direction)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths(DeltaStepping)"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .withExecutorService(Executors.newFixedThreadPool(configuration.getConcurrency()))
                .compute(startNode.getId());

        graph.release();
        return algo.resultStream();
    }

    @Procedure(value = "algo.shortestPath.deltaStepping", mode = Mode.WRITE)
    @Description("CALL algo.shortestPath.deltaStepping(startNode:Node, weightProperty:String, delta:Double" +
            "{label:'labelName', relationship:'relationshipName', defaultValue:1.0, write:true, writeProperty:'sssp'}) " +
            "YIELD loadDuration, evalDuration, writeDuration, nodeCount")
    public Stream<DeltaSteppingProcResult> deltaStepping(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name("delta") Double delta,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Direction direction = configuration.getDirection(Direction.BOTH);

        final DeltaSteppingProcResult.Builder builder = DeltaSteppingProcResult.builder();

        final Graph graph;
        try (ProgressTimer timer = builder.timeLoad()) {
            GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                    .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                    .withRelationshipWeightsFromProperty(
                            propertyName,
                            configuration.getWeightPropertyDefaultValue(Double.MAX_VALUE));

            if(direction == Direction.BOTH) {
                direction = Direction.OUTGOING;
                graphLoader.loadAsUndirected(true).withDirection(direction);
            } else {
                graphLoader.withDirection(direction);
            }

            graph = graphLoader
                    .load(configuration.getGraphImpl());
        }

        if (graph.isEmpty() || startNode == null) {
            graph.release();
            return Stream.empty();
        }

        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        final ShortestPathDeltaStepping algorithm = new ShortestPathDeltaStepping(graph, delta, direction)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths(DeltaStepping)"))
                .withTerminationFlag(terminationFlag)
                .withExecutorService(Pools.DEFAULT);

        builder.timeEval(() -> algorithm.compute(startNode.getId()));

        if (configuration.isWriteFlag()) {
            final double[] shortestPaths = algorithm.getShortestPaths();
            algorithm.release();
            graph.release();
            builder.timeWrite(() -> Exporter
                    .of(api, graph)
                    .withLog(log)
                    .parallel(Pools.DEFAULT, configuration.getWriteConcurrency(), terminationFlag)
                    .build()
                    .write(
                            configuration.get(WRITE_PROPERTY, DEFAULT_TARGET_PROPERTY),
                            shortestPaths,
                            Translators.DOUBLE_ARRAY_TRANSLATOR
                    ));
        }

        return Stream.of(builder
                .withNodeCount(graph.nodeCount())
                .build());
    }
}
