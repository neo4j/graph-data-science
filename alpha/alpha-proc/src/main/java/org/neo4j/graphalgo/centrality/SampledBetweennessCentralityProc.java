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
package org.neo4j.graphalgo.centrality;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.betweenness.BetweennessCentrality;
import org.neo4j.graphalgo.impl.betweenness.RABrandesBetweennessCentrality;
import org.neo4j.graphalgo.impl.betweenness.RandomDegreeSelectionStrategy;
import org.neo4j.graphalgo.impl.betweenness.RandomSelectionStrategy;
import org.neo4j.graphalgo.impl.betweenness.SampledBetweennessCentralityConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

/**
 * Betweenness Centrality Algorithms
 *
 * all procedures accept {@code in, incoming, <, out, outgoing, >, both, <>} as direction
 */
public class SampledBetweennessCentralityProc extends AlgoBaseProc<RABrandesBetweennessCentrality, RABrandesBetweennessCentrality, SampledBetweennessCentralityConfig> {

    private static final String DESCRIPTION = "Sampled Betweenness centrality computes an approximate score for betweenness centrality.";

    @Procedure(name = "gds.alpha.betweenness.sampled.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<BetweennessCentrality.Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<RABrandesBetweennessCentrality, RABrandesBetweennessCentrality, SampledBetweennessCentralityConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        if (computationResult.graph().isEmpty()) {
            return Stream.empty();
        }
        return computationResult.algorithm().resultStream();
    }

    @Procedure(value = "gds.alpha.betweenness.sampled.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<BetweennessCentralityProc.BetweennessCentralityProcResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<RABrandesBetweennessCentrality, RABrandesBetweennessCentrality, SampledBetweennessCentralityConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        BetweennessCentralityProc.BetweennessCentralityProcResult.Builder builder = BetweennessCentralityProc.BetweennessCentralityProcResult
            .builder();

        Graph graph = computationResult.graph();
        RABrandesBetweennessCentrality algo = computationResult.algorithm();
        SampledBetweennessCentralityConfig config = computationResult.config();

        if (graph.isEmpty()) {
            return Stream.of(builder.build());
        }

        computeStats(builder, algo.getCentrality());
        builder.withNodeCount(graph.nodeCount())
            .withComputeMillis(computationResult.computeMillis())
            .withCreateMillis((computationResult.createMillis()));

        graph.release();

        builder.timeWrite(() -> {
            AtomicDoubleArray centrality = algo.getCentrality();
            NodePropertyExporter.of(api, graph, algo.getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build()
                .write(config.writeProperty(), centrality, Translators.ATOMIC_DOUBLE_ARRAY_TRANSLATOR);
        });
        algo.release();
        return Stream.of(builder.build());
    }

    private void computeStats(
        BetweennessCentralityProc.BetweennessCentralityProcResult.Builder builder,
        AtomicDoubleArray centrality
    ) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0.0;
        for (int i = centrality.length() - 1; i >= 0; i--) {
            double c = centrality.get(i);
            if (c < min) {
                min = c;
            }
            if (c > max) {
                max = c;
            }
            sum += c;
        }
        builder.withCentralityMax(max)
            .withCentralityMin(min)
            .withCentralitySum(sum);
    }

    @Override
    protected SampledBetweennessCentralityConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return SampledBetweennessCentralityConfig.of(graphName, maybeImplicitCreate, username, config);
    }

    @Override
    protected void validateGraphCreateConfig(
        GraphCreateConfig graphCreateConfig,
        SampledBetweennessCentralityConfig config
    ) {
        config.validate(graphCreateConfig);
    }

    @Override
    protected AlgorithmFactory<RABrandesBetweennessCentrality, SampledBetweennessCentralityConfig> algorithmFactory(
        SampledBetweennessCentralityConfig config
    ) {
        return new AlphaAlgorithmFactory<RABrandesBetweennessCentrality, SampledBetweennessCentralityConfig>() {
            @Override
            public RABrandesBetweennessCentrality build(
                Graph graph,
                SampledBetweennessCentralityConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new RABrandesBetweennessCentrality(
                    graph,
                    Pools.DEFAULT,
                    configuration.concurrency(),
                    strategy(configuration, graph),
                    configuration.undirected()
                )
                    .withProgressLogger(ProgressLogger.wrap(log, "BetweennessCentrality"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction))
                    .withMaxDepth(configuration.maxDepth());
            }
        };

    }

    private RABrandesBetweennessCentrality.SelectionStrategy strategy(
        SampledBetweennessCentralityConfig configuration,
        Graph graph
    ) {
        switch (configuration.strategy()) {
            case "degree":
                return new RandomDegreeSelectionStrategy(
                    graph,
                    Pools.DEFAULT,
                    configuration.concurrency()
                );
            case "random":
                double probability = configuration.probability();
                if (Double.isNaN(probability)) {
                    probability = Math.log10(graph.nodeCount()) / Math.exp(2);
                }
                return new RandomSelectionStrategy(
                    graph,
                    probability
                );
            default:
                throw new IllegalArgumentException("Unknown selection strategy: " + configuration.strategy());
        }
    }
}
