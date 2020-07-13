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
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.closeness.HarmonicCentralityConfig;
import org.neo4j.graphalgo.impl.harmonic.HarmonicCentrality;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.CentralityScore;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class HarmonicCentralityProc extends AlgoBaseProc<HarmonicCentrality, HarmonicCentrality, HarmonicCentralityConfig> {

    private static final String DESCRIPTION =
        "Harmonic centrality is a way of detecting nodes that are " +
        "able to spread information very efficiently through a graph.";

    @Procedure(name = "gds.alpha.closeness.harmonic.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphNameOrConfig, configuration);

        var algorithm = computationResult.algorithm();
        var graph = computationResult.graph();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        return LongStream.range(0, graph.nodeCount())
            .boxed()
            .map(nodeId -> new StreamResult(graph.toOriginalNodeId(nodeId), algorithm.getCentralityScore(nodeId)));
    }

    @Procedure(value = "gds.alpha.closeness.harmonic.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<CentralityScore.Stats> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphNameOrConfig, configuration);

        var algorithm = computationResult.algorithm();
        var config = computationResult.config();
        var graph = computationResult.graph();

        AbstractResultBuilder<CentralityScore.Stats> builder = new CentralityScore.Stats.Builder()
            .withNodeCount(graph.nodeCount())
            .withConfig(config)
            .withComputeMillis(computationResult.computeMillis())
            .withCreateMillis(computationResult.createMillis());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }

        try (ProgressTimer ignore = ProgressTimer.start(builder::withWriteMillis)) {
            NodePropertyExporter exporter = NodePropertyExporter.builder(api, graph, algorithm.getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, computationResult.config().writeConcurrency())
                .build();

            PropertyTranslator.OfDouble<HarmonicCentrality> translator = HarmonicCentrality::getCentralityScore;

            exporter.write(
                config.writeProperty(),
                computationResult.result(),
                translator
            );
        }

        return Stream.of(builder.build());
    }

    @Override
    protected HarmonicCentralityConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        maybeImplicitCreate = maybeImplicitCreate.map(graphCreateConfig -> {
            if (graphCreateConfig instanceof GraphCreateFromStoreConfig) {
                GraphCreateFromStoreConfig storeConfig = (GraphCreateFromStoreConfig) graphCreateConfig;
                RelationshipProjections.Builder builder = RelationshipProjections.builder();
                storeConfig.relationshipProjections().projections().forEach(
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
        return HarmonicCentralityConfig.of(graphName, maybeImplicitCreate, username, config);
    }

    @Override
    protected AlgorithmFactory<HarmonicCentrality, HarmonicCentralityConfig> algorithmFactory() {
        return new AlphaAlgorithmFactory<>() {
            @Override
            public HarmonicCentrality buildAlphaAlgo(
                Graph graph, HarmonicCentralityConfig configuration, AllocationTracker tracker, Log log
            ) {
                return new HarmonicCentrality(
                    graph,
                    tracker,
                    configuration.concurrency(),
                    Pools.DEFAULT
                );
            }
        };
    }

    public static final class StreamResult {
        public final long nodeId;
        public final double centrality;

        StreamResult(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }
    }
}
