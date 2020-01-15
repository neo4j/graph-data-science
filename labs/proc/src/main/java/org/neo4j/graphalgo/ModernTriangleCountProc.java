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

package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.impl.triangle.ModernIntersectingTriangleCount;
import org.neo4j.graphalgo.impl.triangle.TriangleCountConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ModernTriangleCountProc extends AlgoBaseProc<ModernIntersectingTriangleCount, PagedAtomicIntegerArray, TriangleCountConfig> {

    @Procedure(name = "gds.alpha.triangleCount.stream", mode = READ)
    public Stream<ModernIntersectingTriangleCount.Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ModernIntersectingTriangleCount, PagedAtomicIntegerArray, TriangleCountConfig> computationResult =
            compute(graphNameOrConfig, configuration, true, false);

        Graph graph = computationResult.graph();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        ModernIntersectingTriangleCount algorithm = computationResult.algorithm();
        PagedAtomicIntegerArray triangles = computationResult.result();

        return IntStream.range(0, Math.toIntExact(graph.nodeCount()))
            .mapToObj(i -> new ModernIntersectingTriangleCount.Result(
                graph.toOriginalNodeId(i),
                triangles.get(i),
                algorithm.calculateCoefficient(triangles.get(i), graph.degree(i, Direction.OUTGOING))));
    }

    @Override
    protected TriangleCountConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return TriangleCountConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ModernIntersectingTriangleCount, TriangleCountConfig> algorithmFactory(TriangleCountConfig config) {
        return new AlphaAlgorithmFactory<ModernIntersectingTriangleCount, TriangleCountConfig>() {
            @Override
            public ModernIntersectingTriangleCount build(
                Graph graph,
                TriangleCountConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new ModernIntersectingTriangleCount(
                    graph,
                    Pools.DEFAULT,
                    configuration.concurrency(),
                    AllocationTracker.create()
                )
                    .withProgressLogger(ProgressLogger.wrap(log, "TriangleCount"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            }
        };
    }
}
