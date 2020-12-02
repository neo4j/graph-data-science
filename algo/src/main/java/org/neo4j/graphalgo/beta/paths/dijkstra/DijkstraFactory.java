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
package org.neo4j.graphalgo.beta.paths.dijkstra;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.paths.AllShortestPathsBaseConfig;
import org.neo4j.graphalgo.beta.paths.ShortestPathBaseConfig;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.logging.Log;

public abstract class DijkstraFactory<T extends AlgoBaseConfig & RelationshipWeightConfig> implements AlgorithmFactory<Dijkstra, T> {

    @Override
    public MemoryEstimation memoryEstimation(T configuration) {
        return Dijkstra.memoryEstimation();
    }

    @NotNull
    public static BatchingProgressLogger progressLogger(Graph graph, Log log) {
        return new BatchingProgressLogger(
            log,
            graph.relationshipCount(),
            "Dijkstra",
            1
        );
    }

    public static <T extends ShortestPathBaseConfig> DijkstraFactory<T> sourceTarget() {
        return new DijkstraFactory<T>() {
            @Override
            public Dijkstra build(Graph graph, T configuration, AllocationTracker tracker, Log log) {
                return Dijkstra.sourceTarget(
                    graph,
                    configuration,
                    progressLogger(graph, log),
                    tracker
                );
            }
        };
    }

    public static <T extends AllShortestPathsBaseConfig> DijkstraFactory<T> singleSource() {
        return new DijkstraFactory<T>() {
            @Override
            public Dijkstra build(Graph graph, T configuration, AllocationTracker tracker, Log log) {
                return Dijkstra.singleSource(
                    graph,
                    configuration,
                    progressLogger(graph, log),
                    tracker
                );
            }
        };
    }
}
