/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.scc;

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.traverse.ParallelLocalQueueBFS;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.utils.Converters.longToIntConsumer;
import static org.neo4j.graphalgo.core.utils.Converters.longToIntPredicate;

/**
 * @author mknblch
 */
public class ForwardBackwardScc extends Algorithm<ForwardBackwardScc> {

    private ParallelLocalQueueBFS traverse;
    private IntSet scc = new IntScatterSet();
    private Graph graph;

    public ForwardBackwardScc(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        traverse = new ParallelLocalQueueBFS(graph, executorService, concurrency);
    }

    public ForwardBackwardScc compute(long startNode) {
        int startNodeId = Math.toIntExact(startNode);
        scc.clear();
        // D <- BFS( G(V,E(V)), v)
        final IntScatterSet descendant = new IntScatterSet();
        traverse.bfs(startNodeId,
                Direction.OUTGOING,
                node -> running(),
                longToIntConsumer(descendant::add))
                .awaitTermination();
        getProgressLogger().logProgress(.5);
        // ST <- BFS( G(V, E'(V)), v)
        traverse.reset()
                .bfs(startNodeId,
                        Direction.INCOMING,
                        longToIntPredicate(node -> descendant.contains(node) && running()),
                        longToIntConsumer(scc::add))
                .awaitTermination();
        getProgressLogger().logDone();
        // SCC <- V & ST
        scc.retainAll(descendant);
        return this;
    }

    public IntSet getScc() {
        return scc;
    }

    public Stream<Result> resultStream() {
        return StreamSupport.stream(scc.spliterator(), false)
                .map(node -> new Result(graph.toOriginalNodeId(node.value)));
    }

    @Override
    public ForwardBackwardScc me() {
        return this;
    }

    @Override
    public ForwardBackwardScc release() {
        graph = null;
        traverse = null;
        scc = null;
        return this;
    }

    public class Result {
        public final long nodeId;

        public Result(long nodeId) {
            this.nodeId = nodeId;
        }
    }

}
