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
package org.neo4j.graphalgo.walking;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.walking.RandomWalk;
import org.neo4j.graphalgo.impl.walking.RandomWalkConfig;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStream;
import static org.neo4j.procedure.Mode.READ;

public class RandomWalkProc extends AlgoBaseProc<RandomWalk, Stream<long[]>, RandomWalkConfig> {

    @Procedure(name = "gds.alpha.randomWalk.stream", mode = READ)
    public Stream<WalkResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<RandomWalk, Stream<long[]>, RandomWalkConfig> computationResult =
            compute(graphNameOrConfig, configuration, false, false);

        if (computationResult.graph().isEmpty()) {
            computationResult.graph().release();
            return Stream.empty();
        }

        return computationResult.result()
            .map(nodes -> new WalkResult(
                nodes,
                computationResult.config().path() ? WalkPath.toPath(api, nodes) : null
            ));
    }

    @Override
    protected RandomWalkConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return RandomWalkConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<RandomWalk, RandomWalkConfig> algorithmFactory(RandomWalkConfig config) {
        return new AlphaAlgorithmFactory<RandomWalk, RandomWalkConfig>() {
            @Override
            public RandomWalk build(Graph graph, RandomWalkConfig configuration, AllocationTracker tracker, Log log) {
                Number returnParam = config.returnKey();
                Number inOut = config.inOut();

                RandomWalk.NextNodeStrategy strategy = config.mode().equalsIgnoreCase("random") ?
                    new RandomWalk.RandomNextNodeStrategy(graph, graph) :
                    new RandomWalk.Node2VecStrategy(graph, graph, returnParam.doubleValue(), inOut.doubleValue());

                int limit = (config.walks() == -1)
                    ? Math.toIntExact(graph.nodeCount())
                    : Math.toIntExact(config.walks());

                PrimitiveIterator.OfInt idStream = parallelStream(
                    IntStream.range(0, limit).unordered(),
                    config.concurrency(),
                    stream -> stream.flatMap((s) -> idStream(config.start(), graph, limit)).limit(limit).iterator()
                );

                return new RandomWalk(
                    graph,
                    (int) config.steps(),
                    strategy,
                    configuration.concurrency(),
                    limit,
                    idStream
                )
                    .withProgressLogger(ProgressLogger.wrap(log, "RandomWalk"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            }
        };
    }

    private IntStream idStream(Object start, Graph graph, int limit) {
        int nodeCount = Math.toIntExact(graph.nodeCount());
        if (start instanceof String) {
            String label = start.toString();
            int labelId = transaction.tokenRead().nodeLabel(label);
            int countWithLabel = Math.toIntExact(transaction.dataRead().countsForNodeWithoutTxState(labelId));
            NodeLabelIndexCursor cursor = transaction.cursors().allocateNodeLabelIndexCursor();
            transaction.dataRead().nodeLabelScan(labelId, cursor);
            cursor.next();
            LongStream ids;
            if (limit == -1) {
                ids = LongStream.range(0, countWithLabel).map(i -> cursor.next() ? cursor.nodeReference() : -1L);
            } else {
                int[] indexes = ThreadLocalRandom.current().ints(limit + 1, 0, countWithLabel).sorted().toArray();
                IntStream deltas = IntStream.range(0, limit).map(i -> indexes[i + 1] - indexes[i]);
                ids = deltas.mapToLong(delta -> {
                    while (delta > 0 && cursor.next()) delta--;
                    return cursor.nodeReference();
                });
            }
            return ids.map(graph::toMappedNodeId).mapToInt(Math::toIntExact).onClose(cursor::close);
        } else if (start instanceof Collection) {
            return ((Collection) start)
                .stream()
                .mapToLong(e -> ((Number) e).longValue())
                .map(graph::toMappedNodeId)
                .mapToInt(Math::toIntExact);
        } else if (start instanceof Number) {
            return LongStream.of(((Number) start).longValue()).map(graph::toMappedNodeId).mapToInt(Math::toIntExact);
        } else {
            if (nodeCount < limit) {
                return IntStream.range(0, nodeCount).limit(limit);
            } else {
                return IntStream.generate(() -> ThreadLocalRandom.current().nextInt(nodeCount)).limit(limit);
            }
        }
    }
}
