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
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.degree.DegreeCentrality;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphalgo.results.CentralityScore;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.procedure.Mode.READ;

public class DegreeCentralityProc extends AlgoBaseProc<DegreeCentrality, DegreeCentrality, DegreeCentralityConfig> {

    @Procedure(value = "gds.alpha.degree.write", mode = Mode.WRITE)
    public Stream<CentralityScore.Stats> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<DegreeCentrality, DegreeCentrality, DegreeCentralityConfig> computeResult = compute(
            graphNameOrConfig,
            configuration
        );

        return write(computeResult);
    }

    @Procedure(name = "gds.alpha.degree.stream", mode = READ)
    public Stream<CentralityScore> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<DegreeCentrality, DegreeCentrality, DegreeCentralityConfig> computeResult = compute(
            graphNameOrConfig,
            configuration
        );
        return CentralityUtils.streamResults(computeResult.graph(), computeResult.algorithm().result());
    }

    private Stream<CentralityScore.Stats> write(
        ComputationResult<DegreeCentrality, DegreeCentrality, DegreeCentralityConfig> computeResult
    ) {
        Graph graph = computeResult.graph();
        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(new CentralityScore.Stats(
                    0,
                    0,
                    computeResult.createMillis(),
                    0,
                    false,
                    computeResult.config().writeProperty()
                )
            );
        }

        DegreeCentralityConfig config = computeResult.config();
        DegreeCentrality algorithm = computeResult.algorithm();

        AbstractResultBuilder<CentralityScore.Stats> builder = new CentralityScore.Stats.Builder()
            .withLoadMillis(computeResult.createMillis())
            .withComputeMillis(computeResult.computeMillis())
            .withNodeCount(graph.nodeCount());

        CentralityUtils.write(
            api,
            log,
            computeResult.graph(),
            algorithm.getTerminationFlag(),
            algorithm.result(),
            config,
            builder
        );

        graph.release();
        return Stream.of(builder.build());
    }

    @Override
    protected DegreeCentralityConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return new DegreeCentralityConfigImpl(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    protected AlgorithmFactory<DegreeCentrality, DegreeCentralityConfig> algorithmFactory(DegreeCentralityConfig config) {
        return new AlphaAlgorithmFactory<DegreeCentrality, DegreeCentralityConfig>() {
            @Override
            public DegreeCentrality build(
                Graph graph,
                DegreeCentralityConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                Direction direction = configuration.direction();
                if (direction == BOTH) {
                    direction = OUTGOING;
                }
                return new DegreeCentrality(
                    graph,
                    Pools.DEFAULT,
                    configuration.concurrency(),
                    direction,
                    configuration.isWeighted(),
                    tracker
                );
            }
        };
    }
}
