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
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.MSBFSASPAlgorithm;
import org.neo4j.graphalgo.impl.MSBFSAllShortestPaths;
import org.neo4j.graphalgo.impl.WeightedAllShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class AllShortestPathsProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "algo.allShortestPaths.stream", mode = READ)
    @Description("CALL algo.allShortestPaths.stream(weightProperty:String" +
            "{nodeQuery:'labelName', relationshipQuery:'relationshipName', defaultValue:1.0, concurrency:4}) " +
            "YIELD sourceNodeId, targetNodeId, distance - yields a stream of {sourceNodeId, targetNodeId, distance}")
    public Stream<WeightedAllShortestPaths.Result> allShortestPathsStream(
            @Name(value = "propertyName") String propertyName,
            @Name(value = "config", defaultValue = "{}")
                    Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        Direction direction = configuration.getDirection(Direction.BOTH);

        AllocationTracker tracker = AllocationTracker.create();
        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(configuration.getNodeLabelOrQuery())
                .withOptionalRelationshipType(configuration.getRelationshipOrQuery())
                .withRelationshipProperties(PropertyMapping.of(
                        propertyName,
                        configuration.getWeightPropertyDefaultValue(1.0)))
                .withConcurrency(configuration.getReadConcurrency())
                .withAllocationTracker(tracker);

        if(direction == Direction.BOTH) {
            direction = Direction.OUTGOING;
            graphLoader.undirected().withDirection(direction);
        } else {
            graphLoader.withDirection(direction);
        }

        Graph graph = graphLoader.load(configuration.getGraphImpl());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        final MSBFSASPAlgorithm<?> algo;

        // use MSBFS ASP if no weightProperty is set
        if (null == propertyName || propertyName.isEmpty()) {
            algo = new MSBFSAllShortestPaths(
                        graph,
                        tracker,
                        configuration.getConcurrency(),
                        Pools.DEFAULT,
                        direction);
            algo.withProgressLogger(ProgressLogger.wrap(log, "AllShortestPaths(MultiSource)"));
        } else {
            // weighted ASP otherwise
            algo = new WeightedAllShortestPaths(graph, Pools.DEFAULT, configuration.getConcurrency(), direction)
                    .withProgressLogger(ProgressLogger.wrap(log, "WeightedAllShortestPaths)"));
        }

        return algo.withTerminationFlag(TerminationFlag.wrap(transaction)).resultStream();
    }
}
