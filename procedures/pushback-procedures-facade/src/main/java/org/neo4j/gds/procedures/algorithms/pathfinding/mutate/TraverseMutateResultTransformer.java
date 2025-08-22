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
package org.neo4j.gds.procedures.algorithms.pathfinding.mutate;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.pathfinding.SearchMutateStep;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingMutateResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class TraverseMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<HugeLongArray>, Stream<PathFindingMutateResult>> {

    private final SearchMutateStep searchMutateStep;
    private final Graph graph;
    private final GraphStore graphStore;
    private final Map<String, Object> configuration;

    public TraverseMutateResultTransformer(
        SearchMutateStep searchMutateStep,
        Graph graph,
        GraphStore graphStore,
        Map<String, Object> configuration
    ) {
        this.searchMutateStep = searchMutateStep;
        this.graph = graph;
        this.graphStore = graphStore;
        this.configuration = configuration;
    }

    @Override
    public Stream<PathFindingMutateResult> apply(TimedAlgorithmResult<HugeLongArray> algorithmResult) {
        RelationshipsWritten relationshipsWritten;
        var mutateMillis = new AtomicLong();
        try (var ignored = ProgressTimer.start(mutateMillis::set)) {
            relationshipsWritten = searchMutateStep.execute(graph, graphStore, algorithmResult.result());
        }

        return Stream.of(
            new PathFindingMutateResult(
                0,
                algorithmResult.computeMillis(),
                mutateMillis.get(),
                0,
                relationshipsWritten.value(),
                configuration

            )
        );
    }
}
