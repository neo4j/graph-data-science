/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.paths.dijkstra;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public class DijkstraFactory {

    public static Dijkstra sourceTarget(
        Graph graph,
        long originalNodeId,
        Collection<Long> targetsList,
        boolean trackRelationships,
        Optional<Dijkstra.HeuristicFunction> heuristicFunction,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        long sourceNode = graph.toMappedNodeId(originalNodeId);
        var targets = targetsList.stream().map(graph::toMappedNodeId).collect(Collectors.toList());
        return new Dijkstra(
            graph,
            sourceNode,
            Targets.of(targets),
            trackRelationships,
            heuristicFunction,
            progressTracker,
            terminationFlag
        );
    }

    /**
     * Configure Dijkstra to compute all single-source shortest path.
     */
    public static Dijkstra singleSource(
        Graph graph,
        long originalNodeId,
        boolean trackRelationships,
        Optional<Dijkstra.HeuristicFunction> heuristicFunction,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        return new Dijkstra(
            graph,
            graph.toMappedNodeId(originalNodeId),
            new AllTargets(),
            trackRelationships,
            heuristicFunction,
            progressTracker,
            terminationFlag
        );
    }
}
