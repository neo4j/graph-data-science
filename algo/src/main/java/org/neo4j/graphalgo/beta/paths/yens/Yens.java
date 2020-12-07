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
package org.neo4j.graphalgo.beta.paths.yens;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.paths.dijkstra.Dijkstra;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.beta.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

public final class Yens extends Algorithm<Yens, DijkstraResult> {

    private final Graph graph;
    private final Dijkstra dijkstra;
    private final AllocationTracker tracker;

    /**
     * Configure Yens to compute at most one source-target shortest path.
     */
    public static Yens sourceTarget(
        Graph graph,
        ShortestPathYensBaseConfig config,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        // Init dijkstra algorithm for computing shortest paths
        var dijkstra = Dijkstra.sourceTarget(graph, config, progressLogger, tracker);

        return new Yens(
            graph,
            dijkstra,
            config.k(),
            progressLogger,
            tracker
        );
    }

    private Yens(
        Graph graph,
        Dijkstra dijkstra,
        int k,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.dijkstra = dijkstra;
        this.progressLogger = progressLogger;
        this.tracker = tracker;
    }

    @Override
    public DijkstraResult compute() {
        return null;
    }

    @Override
    public Yens me() {
        return null;
    }

    @Override
    public void release() {

    }
}
