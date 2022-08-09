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
package org.neo4j.gds.graphsampling.samplers.rwr;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ExpectedNodesCounter {

    static Map<Set<NodeLabel>, Long> computeExpectedCountsPerNodeLabelSet(
        Graph inputGraph,
        double samplingRatio,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        progressTracker.beginSubTask("Count node labels");
        progressTracker.setSteps(inputGraph.nodeCount());

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            inputGraph.nodeCount(),
            partition -> new LabelSetCounter(inputGraph, partition, progressTracker),
            Optional.empty()
        );
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .run();

        var totalCounts = new HashMap<Set<NodeLabel>, Long>();
        tasks.forEach(labelSetCounter -> {
            for (var entry : labelSetCounter.labelSetCounts().entrySet()) {
                totalCounts.put(entry.getKey(), entry.getValue() + totalCounts.getOrDefault(entry.getKey(), 0L));
            }
        });
        totalCounts.replaceAll((unused, count) -> Math.round(samplingRatio * count));

        progressTracker.endSubTask("Count node labels");

        return totalCounts;
    }

    static class LabelSetCounter implements Runnable {

        private final Graph inputGraph;
        private final Map<Set<NodeLabel>, Long> counts;
        private final Partition partition;
        private final ProgressTracker progressTracker;

        LabelSetCounter(
            Graph inputGraph,
            Partition partition,
            ProgressTracker progressTracker
        ) {
            this.inputGraph = inputGraph;
            this.progressTracker = progressTracker;
            this.counts = new HashMap<>();
            this.partition = partition;
        }

        @Override
        public void run() {
            partition.consume(
                nodeId -> {
                    // TODO: Can we avoid GC overhead here somehow?
                    var nodeLabelSet = new HashSet<>(inputGraph.nodeLabels(nodeId));
                    counts.put(nodeLabelSet, 1L + counts.getOrDefault(nodeLabelSet, 0L));
                }
            );
            progressTracker.logSteps(partition.nodeCount());
        }

        public Map<Set<NodeLabel>, Long> labelSetCounts() {
            return counts;
        }
    }
}
