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
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.closeness.ClosenessCentralityConfig;
import org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.CentralityScore;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class ClosenessCentralityProc extends AlgoBaseProc<MSClosenessCentrality, MSClosenessCentrality, ClosenessCentralityConfig> {

    private static final String DESCRIPTION =
        "Closeness centrality is a way of detecting nodes that are " +
        "able to spread information very efficiently through a graph.";

    @Procedure(name = "gds.alpha.closeness.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MSClosenessCentrality.Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<MSClosenessCentrality, MSClosenessCentrality, ClosenessCentralityConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        MSClosenessCentrality algorithm = computationResult.algorithm();
        Graph graph = computationResult.graph();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        graph.release();
        return algorithm.resultStream();
    }

    @Procedure(value = "gds.alpha.closeness.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<CentralityScore.Stats> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<MSClosenessCentrality, MSClosenessCentrality, ClosenessCentralityConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        MSClosenessCentrality algorithm = computationResult.algorithm();
        ClosenessCentralityConfig config = computationResult.config();
        Graph graph = computationResult.graph();

        AbstractResultBuilder<CentralityScore.Stats> builder = new CentralityScore.Stats.Builder()
            .withNodeCount(graph.nodeCount())
            .withConfig(config)
            .withComputeMillis(computationResult.computeMillis())
            .withCreateMillis(computationResult.createMillis());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }

        try(ProgressTimer ignore = ProgressTimer.start(builder::withWriteMillis)) {
            NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, algorithm.getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, computationResult.config().writeConcurrency())
                .build();
            algorithm.export(config.writeProperty(), exporter);
        }

        graph.release();
        return Stream.of(builder.build());
    }

    @Override
    protected ClosenessCentralityConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        maybeImplicitCreate = maybeImplicitCreate.map(graphCreateConfig -> {
            if (graphCreateConfig instanceof GraphCreateFromStoreConfig) {
                RelationshipProjections.Builder builder = RelationshipProjections.builder();
                graphCreateConfig.relationshipProjections().projections().forEach(
                    (id, projection) ->
                        builder.putProjection(id, projection.withOrientation(Orientation.UNDIRECTED))
                );
                return ImmutableGraphCreateFromStoreConfig.builder()
                    .from(graphCreateConfig)
                    .relationshipProjections(builder.build())
                    .build();
            }
            return graphCreateConfig;
        });
        return ClosenessCentralityConfig.of(graphName, maybeImplicitCreate, username, config);
    }

    @Override
    protected AlgorithmFactory<MSClosenessCentrality, ClosenessCentralityConfig> algorithmFactory(
        ClosenessCentralityConfig config
    ) {
        return new AlphaAlgorithmFactory<MSClosenessCentrality, ClosenessCentralityConfig>() {
            @Override
            public MSClosenessCentrality buildAlphaAlgo(
                Graph graph,
                ClosenessCentralityConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new MSClosenessCentrality(
                    graph,
                    tracker,
                    configuration.concurrency(),
                    Pools.DEFAULT, configuration.improved()
                );
            }
        };
    }
}
