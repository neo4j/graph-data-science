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
package org.neo4j.graphalgo.shortestpath;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.ShortestPathAStar;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ShortestPathProc extends AlgoBaseProc<ShortestPathAStar, ShortestPathAStar, ShortestPathConfig> {

    @Procedure(name = "algo.shortestPath.astar.stream", mode = READ)
    public Stream<ShortestPathAStar.Result> astarStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
//        @Name("startNode") Node startNode,
//        @Name("endNode") Node endNode,
//        @Name("propertyName") String propertyName,
//        @Name(value = "propertyKeyLat", defaultValue = "latitude") String propertyKeyLat,
//        @Name(value = "propertyKeyLon", defaultValue = "longitude") String propertyKeyLon,
//        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        ComputationResult<ShortestPathAStar, ShortestPathAStar, ShortestPathConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        Graph graph = computationResult.graph();
        if (graph.isEmpty()) {
            return Stream.empty();
        }

        ShortestPathAStar algo = computationResult.algorithm();
        return algo.resultStream();

        /**
         *         ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
         *         Direction direction = configuration.getDirection(Direction.BOTH);
         *
         *         GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
         *             .init(log, configuration.getNodeLabelOrQuery(), configuration.getRelationshipOrQuery(), configuration)
         *             .withRelationshipProperties(PropertyMapping.of(
         *                 propertyName,
         *                 configuration.getWeightPropertyDefaultValue(0.0)
         *             ))
         *             .withDirection(direction);
         *
         *
         *         if (direction == Direction.BOTH) {
         *             direction = Direction.OUTGOING;
         *             graphLoader.undirected().withDirection(direction);
         *         } else {
         *             graphLoader.withDirection(direction);
         *         }
         *
         *         Graph graph = graphLoader.load(configuration.getGraphImpl());
         *
         *         if (graph.isEmpty() || startNode == null || endNode == null) {
         *             graph.release();
         *             return Stream.empty();
         *         }
         *
         *         return new ShortestPathAStar(graph, api)
         *             .withProgressLogger(ProgressLogger.wrap(log, "ShortestPath(AStar)"))
         *             .withTerminationFlag(TerminationFlag.wrap(transaction))
         *             .compute(startNode.getId(), endNode.getId(), propertyKeyLat, propertyKeyLon, direction)
         *             .resultStream();
         */
    }

    @Override
    protected void validateGraphCreateConfig(
        GraphCreateConfig graphCreateConfig,
        ShortestPathConfig config
    ) {
        if (config.direction() == Direction.BOTH) {
            graphCreateConfig.relationshipProjection().projections().entrySet().stream()
                .filter(entry -> entry.getValue().projection() != Projection.UNDIRECTED)
                .forEach(entry -> {
                    throw new IllegalArgumentException(String.format(
                        "Procedure requires relationship projections to be UNDIRECTED. Projection for `%s` uses projection `%s`",
                        entry.getKey().name,
                        entry.getValue().projection()
                    ));
                });
        }
    }

    @Override
    protected ShortestPathConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ShortestPathConfig.of(graphName, maybeImplicitCreate, username, config);
    }

    @Override
    protected AlgorithmFactory<ShortestPathAStar, ShortestPathConfig> algorithmFactory(ShortestPathConfig config) {
        return new AlphaAlgorithmFactory<ShortestPathAStar, ShortestPathConfig>() {
            @Override
            public ShortestPathAStar build(
                Graph graph, ShortestPathConfig configuration, AllocationTracker tracker, Log log
            ) {
                return new ShortestPathAStar(
                    graph,
                    api,
                    configuration.startNodeId(),
                    configuration.endNodeId(),
                    configuration.propertyKeyLat(),
                    configuration.propertyKeyLon(),
                    configuration.resolvedDirection()
                );
            }
        };
    }
}
