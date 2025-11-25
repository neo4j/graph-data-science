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
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.pathfinding.MCMFMutateStep;
import org.neo4j.gds.procedures.algorithms.pathfinding.MCMFMutateResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class MCMFMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<CostFlowResult>, Stream<MCMFMutateResult>> {

    private final MCMFMutateStep mutateStep;
    private final Graph graph;
    private final GraphStore graphStore;
    private final Map<String, Object> configuration;

    MCMFMutateResultTransformer(
        MCMFMutateStep mutateStep,
        Graph graph,
        GraphStore graphStore,
        Map<String, Object> configuration
    ) {
        this.mutateStep = mutateStep;
        this.graph = graph;
        this.graphStore = graphStore;
        this.configuration = configuration;
    }

    @Override
    public Stream<MCMFMutateResult> apply(TimedAlgorithmResult<CostFlowResult> algorithmResult) {

        RelationshipsWritten relationshipsWritten;
        var mutateMillis = new AtomicLong();
        try (var ignored = ProgressTimer.start(mutateMillis::set)) {
            relationshipsWritten = mutateStep.execute(graph, graphStore, algorithmResult.result());
        }
        var flowResult = algorithmResult.result();
        return Stream.of(
            new MCMFMutateResult(
                0,
                algorithmResult.computeMillis(),
                mutateMillis.get(),
                relationshipsWritten.value(),
                flowResult.totalFlow(),
                flowResult.totalCost(),
                configuration
            )
        );
    }
}
