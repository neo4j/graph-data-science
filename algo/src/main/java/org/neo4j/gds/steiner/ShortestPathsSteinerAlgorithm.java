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
package org.neo4j.gds.steiner;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;

public class ShortestPathsSteinerAlgorithm extends Algorithm<SteinerTreeResult> {

    public static long ROOTNODE = -1;
    public static long PRUNED = -2;
    private final Graph graph;
    private final long sourceId;
    private final List<Long> terminals;
    private final int concurrency;
    private final BitSet isTerminal;

    private final double delta;

    public ShortestPathsSteinerAlgorithm(
        Graph graph,
        long sourceId,
        List<Long> terminals,
        double delta,
        int concurrency
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.graph = graph;
        this.sourceId = sourceId;
        this.terminals = terminals;
        this.concurrency = concurrency;
        this.delta = delta;
        this.isTerminal = createTerminalBitSet();
    }

    private BitSet createTerminalBitSet() {
        long maxTerminalId = -1;
        for (long terminalId : terminals) {
            if (terminalId > maxTerminalId) {
                maxTerminalId = terminalId;
            }
        }
        BitSet terminalBitSet = new BitSet(maxTerminalId + 1);
        for (long terminalId : terminals) {
            terminalBitSet.set(terminalId);
        }
        return terminalBitSet;
    }

    @Override
    public SteinerTreeResult compute() {

        HugeLongArray parent = HugeLongArray.newArray(graph.nodeCount());
        HugeDoubleArray parentCost = HugeDoubleArray.newArray(graph.nodeCount());
        ParallelUtil.parallelForEachNode(graph.nodeCount(), concurrency, v -> {
            parentCost.set(v, PRUNED);
            parent.set(v, PRUNED);
        });
        DoubleAdder totalCost = new DoubleAdder();

        var shortestPaths = runShortestPaths();

        initForSource(parent, parentCost);

        shortestPaths.forEachPath(path -> {
            processPath(path, parent, parentCost, totalCost);

        });

        return SteinerTreeResult.of(parent, parentCost, totalCost.doubleValue());
    }

    @Override
    public void release() {

    }

    private void initForSource(HugeLongArray parent, HugeDoubleArray parentCost) {
        parent.set(sourceId, ROOTNODE);
        parentCost.set(sourceId, 0);
    }

    private void processPath(
        PathResult path,
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost
    ) {

        long targetId = path.targetNode();

        if (isTerminal.get(targetId)) {
            var ids = path.nodeIds();
            var costs = path.costs();
            var pastLength = costs.length;
            totalCost.add(path.totalCost());
            for (int j = pastLength - 1; j >= 0; --j) {
                long nodeId = ids[j + 1];
                long parentId = ids[j];
                double cost = costs[j];
                if (j > 0) {
                    cost -= costs[j - 1];
                }
                parent.set(nodeId, parentId);
                parentCost.set(nodeId, cost);

            }
        }
    }

    private DijkstraResult runShortestPaths() {

        // var steinerBasedDijkstra = new SteinerBasedDijkstra(graph, sourceId, isTerminal);
        var steinerBasedDelta = new SteinerBasedDeltaStepping(
            graph,
            sourceId,
            delta,
            isTerminal,
            concurrency,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        return steinerBasedDelta.compute();

    }


}
