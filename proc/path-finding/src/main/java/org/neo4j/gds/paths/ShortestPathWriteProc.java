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
package org.neo4j.gds.paths;

import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.WriteRelationshipConfig;
import org.neo4j.graphalgo.core.TransactionContext;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.ImmutableRelationship;
import org.neo4j.graphalgo.core.write.RelationshipStreamExporter;
import org.neo4j.graphalgo.results.StandardWriteRelationshipsResult;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.stream.Stream;

import static org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.COSTS_KEY;
import static org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.NODE_IDS_KEY;
import static org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.TOTAL_COST_KEY;

public abstract class ShortestPathWriteProc<ALGO extends Algorithm<ALGO, DijkstraResult>, CONFIG extends AlgoBaseConfig & WriteRelationshipConfig & WritePathOptionsConfig>
    extends AlgoBaseProc<ALGO, DijkstraResult, CONFIG> {

    protected Stream<StandardWriteRelationshipsResult> write(ComputationResult<ALGO, DijkstraResult, CONFIG> computationResult) {
        return runWithExceptionLogging("Write relationships failed", () -> {
            var config = computationResult.config();

            var resultBuilder = new StandardWriteRelationshipsResult.Builder()
                .withCreateMillis(computationResult.createMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withConfig(config);

            if (computationResult.isGraphEmpty()) {
                return Stream.of(new StandardWriteRelationshipsResult(computationResult.createMillis(), 0L, 0L, 0L, 0L, config.toMap()));
            }

            var algorithm = computationResult.algorithm();
            var result = computationResult.result();

            var writeRelationshipType = config.writeRelationshipType();

            boolean writeNodeIds = config.writeNodeIds();
            boolean writeCosts = config.writeCosts();

            var graph = computationResult.graph();

            var relationshipStream = result
                .paths()
                .map(pathResult -> ImmutableRelationship.of(
                    pathResult.sourceNode(),
                    pathResult.targetNode(),
                    createValues(graph, pathResult, writeNodeIds, writeCosts)
                ));

            var exporter = RelationshipStreamExporter
                .builder(
                    TransactionContext.of(api, procedureTransaction),
                    computationResult.graph(),
                    relationshipStream,
                    algorithm.getTerminationFlag()
                )
                .withLog(log)
                .build();

            try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
                resultBuilder.withRelationshipsWritten(exporter.write(
                    writeRelationshipType,
                    createKeys(writeNodeIds, writeCosts)
                ));
            }

            return Stream.of(resultBuilder.build());
        });
    }

    private static String[] createKeys(boolean writeNodeIds, boolean writeCosts) {
        if (writeNodeIds && writeCosts) {
            return new String[]{TOTAL_COST_KEY, NODE_IDS_KEY, COSTS_KEY};
        }
        if (writeNodeIds) {
            return new String[]{TOTAL_COST_KEY, NODE_IDS_KEY};
        }
        if (writeCosts) {
            return new String[]{TOTAL_COST_KEY, COSTS_KEY};
        }
        return new String[]{TOTAL_COST_KEY};
    }

    private static Value[] createValues(IdMapping idMapping, PathResult pathResult, boolean writeNodeIds, boolean writeCosts) {
        if (writeNodeIds && writeCosts) {
            return new Value[]{
                Values.doubleValue(pathResult.totalCost()),
                Values.longArray(toOriginalIds(idMapping, pathResult.nodeIds())),
                Values.doubleArray(pathResult.costs())
            };
        }
        if (writeNodeIds) {
            return new Value[]{
                Values.doubleValue(pathResult.totalCost()),
                Values.longArray(toOriginalIds(idMapping, pathResult.nodeIds())),
            };
        }
        if (writeCosts) {
            return new Value[]{
                Values.doubleValue(pathResult.totalCost()),
                Values.doubleArray(pathResult.costs())
            };
        }
        return new Value[]{
            Values.doubleValue(pathResult.totalCost()),
        };
    }

    // Replaces the ids in the given array with the original ids
    private static long[] toOriginalIds(IdMapping idMapping, long[] internalIds) {
        for (int i = 0; i < internalIds.length; i++) {
            internalIds[i] = idMapping.toOriginalNodeId(internalIds[i]);
        }
        return internalIds;
    }
}
