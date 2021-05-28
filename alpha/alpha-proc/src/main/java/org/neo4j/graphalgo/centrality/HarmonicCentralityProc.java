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
package org.neo4j.graphalgo.centrality;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.TransactionContext;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.closeness.HarmonicCentralityConfig;
import org.neo4j.graphalgo.impl.harmonic.HarmonicCentrality;
import org.neo4j.graphalgo.result.AbstractCentralityResultBuilder;
import org.neo4j.graphalgo.results.CentralityScore;
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

        AbstractCentralityResultBuilder<CentralityScore.Stats> builder = new CentralityScore.Stats.Builder(callContext, config.concurrency());

        builder
            .withNodeCount(graph.nodeCount())
            .withConfig(config)
            .withComputeMillis(computationResult.computeMillis())
            .withCreateMillis(computationResult.createMillis());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }

        builder.withCentralityFunction(computationResult.result()::getCentralityScore);

        try (ProgressTimer ignore = ProgressTimer.start(builder::withWriteMillis)) {
            NodePropertyExporter exporter = NodePropertyExporter
                .builder(TransactionContext.of(api, procedureTransaction), graph, algorithm.getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, computationResult.config().writeConcurrency())
                .build();

            var properties = new DoubleNodeProperties() {
                @Override
                public long size() {
                    return computationResult.graph().nodeCount();
                }

                @Override
                public double doubleValue(long nodeId) {
                    return computationResult.result().getCentralityScore(nodeId);
                }
            };

            exporter.write(
                config.writeProperty(),
                properties
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
        return HarmonicCentralityConfig.of(graphName, maybeImplicitCreate.map(AsUndirected::rewrite), username, config);
    }

    @Override
    protected AlgorithmFactory<HarmonicCentrality, HarmonicCentralityConfig> algorithmFactory() {
        return (AlphaAlgorithmFactory<HarmonicCentrality, HarmonicCentralityConfig>) (graph, configuration, tracker, log, eventTracker) ->
            new HarmonicCentrality(
                graph,
                tracker,
                configuration.concurrency(),
                Pools.DEFAULT
            );
    }

    @SuppressWarnings("unused")
    public static final class StreamResult {
        public final long nodeId;
        public final double centrality;

        StreamResult(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }
    }
}
