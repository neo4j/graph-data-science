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
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.walking.NodeWalker;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.graphalgo.results.AbstractWriteBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.ParallelUtil.parallelStream;
import static org.neo4j.procedure.Mode.READ;

public class NodeWalkerProc extends LabsProc {

    @Procedure(name = "algo.randomWalk.stream", mode = READ)
    @Description("CALL algo.randomWalk.stream(start:null=all/[ids]/label, steps, walks, {graph: 'huge/cypher', nodeQuery:nodeLabel/query, relationshipQuery:relType/query, mode:random/node2vec, return:1.0, inOut:1.0, path:false/true concurrency:4, direction:'BOTH'}) " +
            "YIELD nodes, path - computes random walks from given starting points")
    public Stream<WalkResult> randomWalk(
            @Name(value = "start", defaultValue = "null") Object start,
            @Name(value = "steps", defaultValue = "10") long steps,
            @Name(value = "walks", defaultValue = "1") long walks,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();

        AllocationTracker tracker = AllocationTracker.create();

        Direction direction = configuration.getDirection(Direction.BOTH);

        String label = configuration.getNodeLabelOrQuery();
        String relationship = configuration.getRelationshipOrQuery();

        final Graph graph = load(label, relationship, tracker, configuration.getGraphImpl(), statsBuilder, configuration);

        int nodeCount = Math.toIntExact(graph.nodeCount());

        if(nodeCount == 0) {
            graph.release();
            return Stream.empty();
        }


        Number returnParam = configuration.get("return", 1D);
        Number inOut = configuration.get("inOut", 1D);
        NodeWalker.NextNodeStrategy strategy = configuration.get("mode","random").equalsIgnoreCase("random") ?
                new NodeWalker.RandomNextNodeStrategy(graph, graph) :
                new NodeWalker.Node2VecStrategy(graph,graph, returnParam.doubleValue(), inOut.doubleValue());

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int concurrency = configuration.concurrency();

        Boolean returnPath = configuration.get("path", false);

        int limit = (walks == -1) ? nodeCount : Math.toIntExact(walks);

        PrimitiveIterator.OfInt idStream = parallelStream(
                IntStream.range(0, limit).unordered(),
                stream -> stream.flatMap((s) -> idStream(start, graph, limit)).limit(limit).iterator());

        Stream<long[]> randomWalks = new NodeWalker().randomWalk(graph, (int) steps, strategy, terminationFlag, concurrency, limit, idStream);
        return randomWalks
                .map( nodes -> new WalkResult(nodes, returnPath ? WalkPath.toPath(api, nodes) : null));
    }


    private IntStream idStream(@Name(value = "start", defaultValue = "null") Object start, Graph graph, int limit) {
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
                ids = LongStream.range(0, countWithLabel).map( i -> cursor.next() ? cursor.nodeReference() : -1L );
            } else {
                int[] indexes = ThreadLocalRandom.current().ints(limit + 1, 0, countWithLabel).sorted().toArray();
                IntStream deltas = IntStream.range(0, limit).map(i -> indexes[i + 1] - indexes[i]);
                ids = deltas.mapToLong(delta -> { while (delta > 0 && cursor.next()) delta--;return cursor.nodeReference(); });
            }
            return ids.map(graph::toMappedNodeId).mapToInt(Math::toIntExact).onClose(cursor::close);
        } else if (start instanceof Collection) {
            return ((Collection)start).stream().mapToLong(e -> ((Number)e).longValue()).map(graph::toMappedNodeId).mapToInt(Math::toIntExact);
        } else if (start instanceof Number) {
            return LongStream.of(((Number)start).longValue()).map(graph::toMappedNodeId).mapToInt(Math::toIntExact);
        } else {
            if (nodeCount < limit) {
                return IntStream.range(0,nodeCount).limit(limit);
            } else {
                return IntStream.generate(() -> ThreadLocalRandom.current().nextInt(nodeCount)).limit(limit);
            }
        }
    }

    private Graph load(
            String label,
            String relationship,
            AllocationTracker tracker,
            Class<? extends GraphFactory> graphFactory,
            PageRankScore.Stats.Builder statsBuilder, ProcedureConfiguration configuration) {

        GraphLoader graphLoader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, configuration)
                .withAllocationTracker(tracker)
                .withDirection(configuration.getDirection(Direction.BOTH));

        try (ProgressTimer timer = ProgressTimer.start()) {
            Graph graph = graphLoader.load(graphFactory);
            statsBuilder.withNodes(graph.nodeCount());
            return graph;
        }
    }

    public static class PageRankScore {

        public final long nodeId;
        public final Double score;

        public PageRankScore(long nodeId, final Double score) {
            this.nodeId = nodeId;
            this.score = score;
        }

        // TODO: return number of relationships as well
        //  the Graph API doesn't expose this value yet
        public static final class Stats {
            public final long nodes, iterations, loadMillis, computeMillis, writeMillis;
            public final double dampingFactor;
            public final boolean write;
            public final String writeProperty;

            Stats(
                long nodes,
                long iterations,
                long loadMillis,
                long computeMillis,
                long writeMillis,
                double dampingFactor,
                boolean write,
                String writeProperty) {
                this.nodes = nodes;
                this.iterations = iterations;
                this.loadMillis = loadMillis;
                this.computeMillis = computeMillis;
                this.writeMillis = writeMillis;
                this.dampingFactor = dampingFactor;
                this.write = write;
                this.writeProperty = writeProperty;
            }

            public static final class Builder extends AbstractWriteBuilder<Stats> {
                private long nodes;
                private long iterations;
                private double dampingFactor;
                private boolean write;
                private String writeProperty;

                public Builder withNodes(long nodes) {
                    this.nodes = nodes;
                    return this;
                }

                public Builder withIterations(long iterations) {
                    this.iterations = iterations;
                    return this;
                }

                public Builder withDampingFactor(double dampingFactor) {
                    this.dampingFactor = dampingFactor;
                    return this;
                }

                @Override
                public Builder withWrite(boolean write) {
                    this.write = write;
                    return this;
                }

                @Override
                public Builder withProperty(String writeProperty) {
                    this.writeProperty = writeProperty;
                    return this;
                }

                public PageRankScore.Stats build() {
                    return new PageRankScore.Stats(
                        nodes,
                        iterations,
                        loadMillis,
                        computeMillis,
                        writeMillis,
                        dampingFactor,
                        write,
                        writeProperty);
                }
            }
        }
    }

}
