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

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.ShortestPaths;
import org.neo4j.graphalgo.results.ShortestPathResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ShortestPathsProc extends LabsProc {

    public static final String WRITE_PROPERTY = "writeProperty";
    public static final String DEFAULT_TARGET_PROPERTY = "sssp";

    @Procedure(name = "algo.shortestPaths.stream", mode = READ)
    @Description("CALL algo.shortestPaths.stream(startNode:Node, weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0}) " +
            "YIELD nodeId, distance - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPaths.Result> dijkstraStream(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, configuration.getNodeLabelOrQuery(),configuration.getRelationshipOrQuery(),configuration)
                .withRelationshipProperties(PropertyMapping.of(
                        propertyName,
                        configuration.getWeightPropertyDefaultValue(1.0)))
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());

        if (graph.isEmpty() || startNode == null) {
            graph.release();
            return Stream.empty();
        }

        final ShortestPaths algo = new ShortestPaths(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths"))
                .withTerminationFlag(TerminationFlag.wrap(transaction))
                .compute(startNode.getId());
        graph.release();
        return algo.resultStream();
    }

    @Procedure(value = "algo.shortestPaths", mode = Mode.WRITE)
    @Description("CALL algo.shortestPaths(startNode:Node, weightProperty:String" +
            "{write:true, targetProperty:'path', nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0}) " +
            "YIELD loadDuration, evalDuration, writeDuration, nodeCount, targetProperty - yields nodeCount, totalCost, loadDuration, evalDuration")
    public Stream<ShortestPathResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        ShortestPathResult.Builder builder = ShortestPathResult.builder();

        ProgressTimer load = builder.timeLoad();
        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
                .withRelationshipProperties(PropertyMapping.of(
                        propertyName,
                        configuration.getWeightPropertyDefaultValue(1.0)))
                .withDirection(Direction.OUTGOING)
                .load(configuration.getGraphImpl());
        load.stop();

        if (graph.isEmpty() || startNode == null) {
            graph.release();
            return Stream.of(builder.build());
        }

        final TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        final ShortestPaths algorithm = new ShortestPaths(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "ShortestPaths"))
                .withTerminationFlag(terminationFlag);

        builder.timeEval(() -> algorithm.compute(startNode.getId()));

        if (configuration.isWriteFlag()) {
            builder.timeWrite(() -> {
                final IntDoubleMap shortestPaths = algorithm.getShortestPaths();
                algorithm.release();
                graph.release();
                Exporter.of(api, graph)
                        .withLog(log)
                        .parallel(Pools.DEFAULT, configuration.getWriteConcurrency(), terminationFlag)
                        .build()
                        .write(
                                configuration.getWriteProperty(DEFAULT_TARGET_PROPERTY),
                                shortestPaths,
                                Translators.INT_DOUBLE_MAP_TRANSLATOR
                        );
            });
        }

        return Stream.of(builder.build());
    }

}
