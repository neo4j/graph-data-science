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
package org.neo4j.gds.influenceMaximization;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.influenceMaximization.Greedy;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.InfluenceMaximizationResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GreedyProc extends AlgoBaseProc<Greedy, Greedy, InfluenceMaximizationConfig> {
    private static final String DESCRIPTION = "The Greedy algorithm aims to find k nodes that maximize the expected spread of influence in the network.";

    @Procedure(name = "gds.alpha.influenceMaximization.greedy.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<InfluenceMaximizationResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        ComputationResult<Greedy, Greedy, InfluenceMaximizationConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        if (computationResult.graph().isEmpty()) {
            computationResult.graph().release();
            return Stream.empty();
        }

        computationResult.graph().release();
        return computationResult.algorithm().resultStream();
    }

//    @Procedure(name = "gds.alpha.influenceMaximization.greedy.stats", mode = READ)
    @Description(DESCRIPTION)
    public Stream<InfluenceMaximizationResult.Stats> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Greedy, Greedy, InfluenceMaximizationConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        InfluenceMaximizationConfig config = computationResult.config();
        Graph graph = computationResult.graph();

        AbstractResultBuilder<InfluenceMaximizationResult.Stats> builder = new InfluenceMaximizationResult.Stats.Builder()
            .withNodeCount(graph.nodeCount())
            .withConfig(config)
            .withComputeMillis(computationResult.computeMillis());

        return Stream.of(builder.build());
    }

    @Override
    protected InfluenceMaximizationConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return new InfluenceMaximizationConfigImpl(graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Greedy, InfluenceMaximizationConfig> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            protected String taskName() {
                return "Greedy";
            }

            @Override
            protected Greedy build(
                Graph graph,
                InfluenceMaximizationConfig configuration,
                AllocationTracker allocationTracker,
                ProgressTracker progressTracker
            ) {
                return new Greedy(
                    graph,
                    configuration.seedSetSize(),
                    configuration.propagationProbability(),
                    configuration.monteCarloSimulations(),
                    Pools.DEFAULT,
                    configuration.concurrency(),
                    allocationTracker
                );
            }
        };
    }
}
