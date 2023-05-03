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
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

public class ShortestPathsSteinerAlgorithm extends Algorithm<SteinerTreeResult> {

    public static final long ROOT_NODE = -1;
    public static final long PRUNED = -2;
    private final Graph graph;
    private final long sourceId;
    private final List<Long> terminals;
    private final int concurrency;
    private final BitSet isTerminal;
    private final boolean applyRerouting;
    private final double delta;
    private final ExecutorService executorService;
    private final int binSizeThreshold;
    private final HugeLongArray examinationQueue;
    private final LongAdder indexQueue;
    public ShortestPathsSteinerAlgorithm(
        Graph graph,
        long sourceId,
        List<Long> terminals,
        double delta,
        int concurrency,
        boolean applyRerouting,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.sourceId = sourceId;
        this.terminals = terminals;
        this.concurrency = concurrency;
        this.delta = delta;
        this.isTerminal = createTerminals();
        this.applyRerouting = applyRerouting;
        this.executorService = executorService;
        this.binSizeThreshold = SteinerBasedDeltaStepping.BIN_SIZE_THRESHOLD;
        this.examinationQueue = createExaminationQueue(graph, applyRerouting, terminals.size());
        this.indexQueue = new LongAdder();
    }

    @TestOnly
    ShortestPathsSteinerAlgorithm(
        Graph graph,
        long sourceId,
        List<Long> terminals,
        double delta,
        int concurrency,
        boolean applyRerouting,
        int binSizeThreshold,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.sourceId = sourceId;
        this.terminals = terminals;
        this.concurrency = concurrency;
        this.delta = delta;
        this.isTerminal = createTerminals();
        this.applyRerouting = applyRerouting;
        this.executorService = executorService;
        this.binSizeThreshold = binSizeThreshold;
        this.examinationQueue = createExaminationQueue(graph, applyRerouting, terminals.size());
        this.indexQueue = new LongAdder();
        
    }

    private BitSet createTerminals() {
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
        progressTracker.beginSubTask("SteinerTree");
        progressTracker.beginSubTask("Traverse");
        HugeLongArray parent = HugeLongArray.newArray(graph.nodeCount());
        HugeDoubleArray parentCost = HugeDoubleArray.newArray(graph.nodeCount());
        ParallelUtil.parallelForEachNode(graph.nodeCount(), concurrency, v -> {
            parentCost.set(v, PRUNED);
            parent.set(v, PRUNED);
        });
        DoubleAdder totalCost = new DoubleAdder();
        LongAdder effectiveNodeCount = new LongAdder();
        LongAdder terminalsReached = new LongAdder();

        effectiveNodeCount.increment(); //sourceNode is always in the solution
        var shortestPaths = runShortestPaths();

        initForSource(parent, parentCost);

        shortestPaths.forEachPath(path -> {
            processPath(path, parent, parentCost, totalCost, effectiveNodeCount);
            terminalsReached.increment();
        });
        progressTracker.endSubTask("Traverse");

        if (applyRerouting) {
            var rerouter = ReroutingSupplier.createRerouter(
                graph,
                sourceId,
                terminals,
                isTerminal,
                examinationQueue,
                indexQueue,
                concurrency,
                progressTracker
            );
            rerouter.reroute(parent, parentCost, totalCost, effectiveNodeCount);
        }

        progressTracker.endSubTask("SteinerTree");
        return SteinerTreeResult.of(
            parent,
            parentCost,
            totalCost.doubleValue(),
            effectiveNodeCount.longValue(),
            terminalsReached.longValue()
        );
    }

    private void initForSource(HugeLongArray parent, HugeDoubleArray parentCost) {
        parent.set(sourceId, ROOT_NODE);
        parentCost.set(sourceId, 0);
    }

    private void processPath(
        PathResult path,
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        LongAdder effectiveNodeCount
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
                handleNextNode(nodeId);
                parentCost.set(nodeId, cost);
                effectiveNodeCount.increment();
            }
            handleNextNode(PRUNED);

        }
    }

    private PathFindingResult runShortestPaths() {
        var steinerBasedDelta = new SteinerBasedDeltaStepping(
            graph,
            sourceId,
            delta,
            isTerminal,
            concurrency,
            binSizeThreshold,
            executorService,
            progressTracker
        );

        return steinerBasedDelta.compute();
    }

    private HugeLongArray createExaminationQueue(Graph graph, boolean applyRerouting, long numberOfTerminals) {
        if (!applyRerouting || graph.characteristics().isUndirected() || !graph.characteristics().isInverseIndexed()) {
            return null;
        }
        return HugeLongArray.newArray(graph.nodeCount() + numberOfTerminals);

    }

    private long nextQueuePosition() {
        var toReturn = indexQueue.longValue();
        indexQueue.increment();
        return toReturn;
    }

    private void handleNextNode(long value) {
        if (examinationQueue != null) {
            examinationQueue.set(nextQueuePosition(), value);
        }
    }


}
