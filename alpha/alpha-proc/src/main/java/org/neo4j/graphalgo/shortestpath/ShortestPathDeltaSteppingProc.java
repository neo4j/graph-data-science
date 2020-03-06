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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.ShortestPathDeltaStepping;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphalgo.results.DeltaSteppingProcResult;
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
 * Delta-Stepping is a non-negative single source shortest paths (NSSSP) algorithm
 * to calculate the length of the shortest paths from a starting node to all other
 * nodes in the graph. It can be tweaked using the delta-parameter which controls
 * the grade of concurrency.<br>
 * <p>
 * More information in:<br>
 * <p>
 * <a href="https://arxiv.org/pdf/1604.02113v1.pdf">https://arxiv.org/pdf/1604.02113v1.pdf</a><br>
 * <a href="https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf">https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf</a><br>
 * <a href="http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf">http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf</a><br>
 * <a href="http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf">http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf</a>
 */
public class ShortestPathDeltaSteppingProc extends AlgoBaseProc<ShortestPathDeltaStepping, ShortestPathDeltaStepping, ShortestPathDeltaSteppingConfig> {

    private static final String DESCRIPTION = "Delta-Stepping is a non-negative single source shortest paths (NSSSP) algorithm.";

    @Procedure(name = "gds.alpha.shortestPath.deltaStepping.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ShortestPathDeltaStepping.DeltaSteppingResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        ComputationResult<ShortestPathDeltaStepping, ShortestPathDeltaStepping, ShortestPathDeltaSteppingConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        if (computationResult.graph().isEmpty()) {
            return Stream.empty();
        }

        return computationResult.result().resultStream();
    }

    @Procedure(value = "gds.alpha.shortestPath.deltaStepping.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<DeltaSteppingProcResult> deltaStepping(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ShortestPathDeltaStepping, ShortestPathDeltaStepping, ShortestPathDeltaSteppingConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        Graph graph = computationResult.graph();
        ShortestPathDeltaStepping algorithm = computationResult.algorithm();
        ShortestPathDeltaSteppingConfig config = computationResult.config();

        AbstractResultBuilder<DeltaSteppingProcResult> builder = DeltaSteppingProcResult.builder();

        if (graph.isEmpty()) {
            return Stream.empty();
        }

        builder.timeWrite(() -> {
            NodePropertyExporter
                .of(api, graph, algorithm.getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build()
                .write(
                    config.writeProperty(),
                    algorithm.getShortestPaths(),
                    Translators.DOUBLE_ARRAY_TRANSLATOR
                );
        });

        return Stream.of(builder
            .withNodeCount(graph.nodeCount())
            .build());
    }

    @Override
    protected ShortestPathDeltaSteppingConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ShortestPathDeltaSteppingConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ShortestPathDeltaStepping, ShortestPathDeltaSteppingConfig> algorithmFactory(
        ShortestPathDeltaSteppingConfig config
    ) {
        return new AlphaAlgorithmFactory<ShortestPathDeltaStepping, ShortestPathDeltaSteppingConfig>() {
            @Override
            public ShortestPathDeltaStepping build(
                Graph graph,
                ShortestPathDeltaSteppingConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new ShortestPathDeltaStepping(
                    graph,
                    configuration.startNode(),
                    configuration.delta()
                );
            }
        };
    }
}
