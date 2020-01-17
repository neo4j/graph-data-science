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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.ShortestPathAStar;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ShortestPathProc extends LabsProc {

    @Procedure(name = "algo.shortestPath.astar.stream", mode = READ)
    @Description("CALL algo.shortestPath.astar.stream(startNode:Node, endNode:Node, weightProperty:String, propertyKeyLat:String," +
                 "propertyKeyLon:String, {nodeQuery:'labelName', relationshipQuery:'relationshipName', direction:'BOTH', defaultValue:1.0}) " +
                 "YIELD nodeId, cost - yields a stream of {nodeId, cost} from start to end (inclusive)")
    public Stream<ShortestPathAStar.Result> astarStream(
        @Name("startNode") Node startNode,
        @Name("endNode") Node endNode,
        @Name("propertyName") String propertyName,
        @Name(value = "propertyKeyLat", defaultValue = "latitude") String propertyKeyLat,
        @Name(value = "propertyKeyLon", defaultValue = "longitude") String propertyKeyLon,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        Direction direction = configuration.getDirection(Direction.BOTH);

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
            .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
            .withRelationshipProperties(PropertyMapping.of(
                propertyName,
                configuration.getWeightPropertyDefaultValue(1.0)
            ))
            .withDirection(direction);


        if (direction == Direction.BOTH) {
            direction = Direction.OUTGOING;
            graphLoader.undirected().withDirection(direction);
        } else {
            graphLoader.withDirection(direction);
        }

        final Graph graph = graphLoader.load(configuration.getGraphImpl());

        if (graph.isEmpty() || startNode == null || endNode == null) {
            graph.release();
            return Stream.empty();
        }

        return new ShortestPathAStar(graph, api, startNode.getId(), endNode.getId(), propertyKeyLat, propertyKeyLon, direction)
            .withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(AStar)"))
            .withTerminationFlag(TerminationFlag.wrap(transaction))
            .compute()
            .resultStream();
    }
}
