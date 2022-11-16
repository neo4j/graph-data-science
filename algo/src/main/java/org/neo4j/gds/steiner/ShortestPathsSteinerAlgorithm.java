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
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Function;

public class ShortestPathsSteinerAlgorithm extends Algorithm<SteinerTreeResult> {

    public static long ROOTNODE = -1;
    public static long PRUNED = -2;

    private final Graph graph;
    private final long sourceId;
    private final List<Long> terminals;
    private final int concurrency;
    private final BitSet isTerminal;
    private final String relationshipWeightProperty;


    public ShortestPathsSteinerAlgorithm(
        Graph graph,
        long sourceId,
        List<Long> terminals,
        int concurrency,
        String relationshipWeightProperty
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.graph = graph;
        this.sourceId = sourceId;
        this.terminals = terminals;
        this.concurrency = concurrency;
        this.relationshipWeightProperty = relationshipWeightProperty;
        isTerminal = createTerminalBitSet();
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
        parentCost.fill(PRUNED);
        DoubleAdder totalCost = new DoubleAdder();

        var shortestPaths = runShortestPaths();

        BitSet covered = new BitSet(graph.nodeCount());

        initForSource(parent, parentCost, covered);

        shortestPaths.forEachPath(path -> {
            processPath(path, parent, covered);

        });

        ignoreAllOtherNodes(covered, parent);
        calculateWeights(covered, parent, parentCost, totalCost);
        return SteinerTreeResult.of(parent, parentCost, totalCost.doubleValue());
    }

    @Override
    public void release() {

    }

    private void initForSource(HugeLongArray parent, HugeDoubleArray parentCost, BitSet covered) {
        covered.set(sourceId);
        parent.set(sourceId, ROOTNODE);
        parentCost.set(sourceId, 0);
    }

    private void ignoreAllOtherNodes(BitSet covered, HugeLongArray parent) {

        ParallelUtil.parallelForEachNode(graph.nodeCount(), concurrency, nodeId -> {
            if (!covered.get(nodeId)) {
                parent.set(nodeId, PRUNED);
            }
        });

    }

    private void calculateWeights(
        BitSet covered,
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost
    ) {
        HugeLongArray coveredNodes = findCoveredNodes(covered);
        Function<Long, Integer> customDegreeFunction = x -> graph.degree(coveredNodes.get(x));
        var tasks = PartitionUtils.customDegreePartitionWithBatchSize(
            concurrency,
            coveredNodes.size(),
            customDegreeFunction,
            partition -> new CostCalculationTask(graph, partition, coveredNodes, parent, parentCost, totalCost),
            Optional.empty(),
            Optional.empty()
        );
        RunWithConcurrency
            .builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(Pools.DEFAULT)
            .run();

    }

    private HugeLongArray findCoveredNodes(BitSet covered) {
        var coveredNodes = HugeLongArray.newArray(covered.cardinality());
        long nodeIndex = covered.nextSetBit(0);
        long indexId = 0;
        while (nodeIndex != -1) {
            coveredNodes.set(indexId++, nodeIndex);
            nodeIndex = covered.nextSetBit(nodeIndex + 1);
        }
        return coveredNodes;
    }

    private void processPath(
        PathResult path,
        HugeLongArray parent,
        BitSet covered
    ) {

        long targetId = path.targetNode();

        if (isTerminal.get(targetId)) {
            var ids = path.nodeIds();
            var pathLength = ids.length;

            for (int j = pathLength - 1; j >= 0; --j) {
                long nodeId = ids[j];
                if (covered.get(nodeId)) {
                    break;
                } else {
                    covered.set(nodeId);
                    long parentId = ids[j - 1];
                    parent.set(nodeId, parentId);
                }
            }
        }
    }

    private DijkstraResult runShortestPaths() {

        var steinerBasedDijkstra = new SteinerBasedDijkstra(graph, sourceId, isTerminal);
        return steinerBasedDijkstra.compute();

    }


}
