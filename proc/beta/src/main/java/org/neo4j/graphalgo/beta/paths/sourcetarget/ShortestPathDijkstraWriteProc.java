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
package org.neo4j.graphalgo.beta.paths.sourcetarget;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.beta.paths.PathResult;
import org.neo4j.graphalgo.beta.paths.WriteResult;
import org.neo4j.graphalgo.beta.paths.dijkstra.Dijkstra;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraFactory;
import org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.ImmutableRelationship;
import org.neo4j.graphalgo.core.write.RelationshipStreamExporter;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.COSTS_KEY;
import static org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.NODE_IDS_KEY;
import static org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.TOTAL_COST_KEY;
import static org.neo4j.graphalgo.beta.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.of;
import static org.neo4j.graphalgo.beta.paths.sourcetarget.ShortestPathDijkstraProc.DIJKSTRA_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class ShortestPathDijkstraWriteProc extends AlgoBaseProc<Dijkstra, DijkstraResult, ShortestPathDijkstraWriteConfig> {

    @Procedure(name = "gds.beta.shortestPath.dijkstra.write", mode = WRITE)
    @Description(DIJKSTRA_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphNameOrConfig, configuration));
    }

    @Procedure(name = "gds.beta.shortestPath.dijkstra.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> writeEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    protected Stream<WriteResult> write(ComputationResult<Dijkstra, DijkstraResult, ShortestPathDijkstraWriteConfig> computationResult) {
        return runWithExceptionLogging("Write relationships failed", () -> {
            var config = computationResult.config();

            var resultBuilder = new WriteResult.Builder()
                .withCreateMillis(computationResult.createMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withConfig(config);

            if (computationResult.isGraphEmpty()) {
                return Stream.of(new WriteResult(computationResult.createMillis(), 0L, 0L, 0L, config.toMap()));
            }

            var algorithm = computationResult.algorithm();
            var result = computationResult.result();

            var writeRelationshipType = config.writeRelationshipType();

            var relationshipStream = result
                .paths()
                .takeWhile(pathResult -> pathResult != PathResult.EMPTY)
                .map(pathResult -> ImmutableRelationship.of(
                    pathResult.sourceNode(),
                    pathResult.targetNode(),
                    new Value[]{
                        Values.doubleValue(pathResult.totalCost()),
                        Values.longArray(pathResult.nodeIds()),
                        Values.doubleArray(pathResult.costs())
                    }
                ));

            var exporter = RelationshipStreamExporter
                .builder(api, computationResult.graph(), relationshipStream, algorithm.getTerminationFlag())
                .withLog(log)
                .build();

            try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
                resultBuilder.withRelationshipsWritten(exporter.write(
                    writeRelationshipType,
                    TOTAL_COST_KEY,
                    NODE_IDS_KEY,
                    COSTS_KEY
                ));
            }

            return Stream.of(resultBuilder.build());
        });
    }

    @Override
    protected ShortestPathDijkstraWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Dijkstra, ShortestPathDijkstraWriteConfig> algorithmFactory() {
        return DijkstraFactory.sourceTarget();
    }
}
