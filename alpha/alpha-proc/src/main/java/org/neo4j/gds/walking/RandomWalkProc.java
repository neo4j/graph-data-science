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
package org.neo4j.gds.walking;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.walking.RandomWalk;
import org.neo4j.gds.impl.walking.RandomWalkConfig;
import org.neo4j.gds.impl.walking.WalkPath;
import org.neo4j.gds.impl.walking.WalkResult;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.procedure.Description;
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

import static org.neo4j.gds.core.concurrency.ParallelUtil.parallelStream;
import static org.neo4j.procedure.Mode.READ;

public class RandomWalkProc extends AlgoBaseProc<RandomWalk, Stream<long[]>, RandomWalkConfig> {

    private static final String DESCRIPTION =
        "Random Walk is an algorithm that provides random paths in a graph. " +
        "It’s similar to how a drunk person traverses a city.";

    @Procedure(name = "gds.alpha.randomWalk.stream", mode = READ)
    @Description(DESCRIPTION)
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
                computationResult.config().path() ? WalkPath.toPath(transaction, nodes) : null
            ));
    }

    @Override
    protected RandomWalkConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return RandomWalkConfig.of(graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<RandomWalk, RandomWalkConfig> algorithmFactory() {
        return new AlgorithmFactory<>() {
            @Override
            protected String taskName() {
                return "RandomWalk";
            }

            @Override
            protected RandomWalk build(
                Graph graph,
                RandomWalkConfig configuration,
                AllocationTracker allocationTracker,
                ProgressTracker progressTracker
            ) {
                Number returnParam = configuration.returnKey();
                Number inOut = configuration.inOut();

                RandomWalk.NextNodeStrategy strategy = configuration.mode().equalsIgnoreCase("random") ?
                    new RandomWalk.RandomNextNodeStrategy(graph, graph) :
                    new RandomWalk.Node2VecStrategy(graph, graph, returnParam.doubleValue(), inOut.doubleValue());

                int limit = (configuration.walks() == -1)
                    ? Math.toIntExact(graph.nodeCount())
                    : Math.toIntExact(configuration.walks());

                PrimitiveIterator.OfInt idStream = parallelStream(
                    IntStream.range(0, limit).unordered(),
                    configuration.concurrency(),
                    stream -> stream
                        .flatMap((s) -> idStream(configuration.start(), graph, limit))
                        .limit(limit)
                        .iterator()
                );

                return new RandomWalk(
                    graph,
                    (int) configuration.steps(),
                    strategy,
                    configuration.concurrency(),
                    limit,
                    idStream
                )
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
            NodeLabelIndexCursor cursor = Neo4jProxy.allocateNodeLabelIndexCursor(transaction);
            Neo4jProxy.nodeLabelScan(transaction, labelId, cursor);
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
            return ids.map(graph::safeToMappedNodeId).mapToInt(Math::toIntExact).onClose(cursor::close);
        } else if (start instanceof Collection) {
            return ((Collection<?>) start)
                .stream()
                .mapToLong(e -> ((Number) e).longValue())
                .map(graph::safeToMappedNodeId)
                .mapToInt(Math::toIntExact);
        } else if (start instanceof Number) {
            return LongStream.of(((Number) start).longValue()).map(graph::safeToMappedNodeId).mapToInt(Math::toIntExact);
        } else {
            if (nodeCount < limit) {
                return IntStream.range(0, nodeCount).limit(limit);
            } else {
                return IntStream.generate(() -> ThreadLocalRandom.current().nextInt(nodeCount)).limit(limit);
            }
        }
    }
}
