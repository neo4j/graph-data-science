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
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.closeness.ClosenessCentralityConfig;
import org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.results.CentralityScore;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ClosenessCentralityProc extends AlgoBaseProc<MSClosenessCentrality, MSClosenessCentrality, ClosenessCentralityConfig> {

    @Procedure(name = "gds.alpha.closeness.stream", mode = READ)
    public Stream<MSClosenessCentrality.Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<MSClosenessCentrality, MSClosenessCentrality, ClosenessCentralityConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        if (computationResult.graph().isEmpty()) {
            return Stream.empty();
        }

        computationResult.graph().releaseProperties();
        return computationResult.algorithm().resultStream();
    }

    @Procedure(value = "gds.alpha.closeness.write", mode = Mode.WRITE)
    public Stream<CentralityScore.Stats> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<MSClosenessCentrality, MSClosenessCentrality, ClosenessCentralityConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        CentralityScore.Stats.Builder builder = new CentralityScore.Stats.Builder();

        if (computationResult.graph().isEmpty()) {
            return Stream.of(builder.build());
        }

        ClosenessCentralityConfig config = computationResult.config();
        builder.withNodeCount(computationResult.graph().nodeCount())
            .withWriteProperty(config.writeProperty())
            .withComputeMillis(computationResult.computeMillis())
            .withLoadMillis(computationResult.createMillis());

        String writeProperty = config.writeProperty();
        MSClosenessCentrality algorithm = computationResult.algorithm();

        builder.timeWrite(() -> {
            NodePropertyExporter exporter = NodePropertyExporter.of(api, computationResult.graph(), algorithm
                .getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, computationResult.config().writeConcurrency())
                .build();
            algorithm.export(writeProperty, exporter);
        });
        algorithm.release();

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
                graphCreateConfig.relationshipProjection().projections().forEach(
                    (id, projection) ->
                        builder.putProjection(id, projection.withProjection(Projection.UNDIRECTED))
                );
                return ImmutableGraphCreateFromStoreConfig.builder()
                    .from(graphCreateConfig)
                    .relationshipProjection(builder.build())
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
            public MSClosenessCentrality build(
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
