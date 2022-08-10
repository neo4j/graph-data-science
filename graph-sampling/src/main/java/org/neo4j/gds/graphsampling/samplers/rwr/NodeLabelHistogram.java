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

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.procedures.LongLongProcedure;
import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public final class NodeLabelHistogram {

    @ValueClass
    interface Result {
        NodeLabel[] availableNodeLabels();

        LongLongHashMap histogram();
    }

    static Result compute(Graph inputGraph, int concurrency, ProgressTracker progressTracker) {
        progressTracker.beginSubTask("Count node labels");
        progressTracker.setSteps(inputGraph.nodeCount());

        var availableNodeLabels = inputGraph.availableNodeLabels()
            .stream()
            .sorted(Comparator.comparing(ElementIdentifier::name))
            .collect(Collectors.toList())
            .toArray(NodeLabel[]::new);

        var localLabelCounts = new ConcurrentLinkedQueue<LongLongHashMap>();

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            inputGraph.nodeCount(),
            partition -> (Runnable) () -> {
                var labelCount = new LongLongHashMap();
                partition.consume(nodeId -> {
                    labelCount.addTo(
                        encodedLabelCombination(inputGraph, availableNodeLabels, nodeId),
                        1
                    );
                });
                localLabelCounts.add(labelCount);
            },
            Optional.empty()
        );
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .run();

        var totalCounts = new LongLongHashMap();
        localLabelCounts.forEach(labelCount -> labelCount
            .forEach((LongLongProcedure) (labelCombination, count) -> totalCounts.put(
                labelCombination,
                totalCounts.getOrDefault(labelCombination, 0) + count
            )));

        progressTracker.endSubTask("Count node labels");

        return ImmutableResult.builder()
            .availableNodeLabels(availableNodeLabels)
            .histogram(totalCounts)
            .build();
    }

    /**
     * Represents the node labels of the given node as a bitvector of length 64.
     * A bit at index i is set if the node has label at availableNodeLabels[i].
     *
     * @return a long representing the labels of the given node
     */
    static long encodedLabelCombination(IdMap idMap, NodeLabel[] availableNodeLabels, long nodeId) {
        long labelCombination = 0L;

        for (int i = 0; i < availableNodeLabels.length; i++) {
            if (idMap.hasLabel(nodeId, availableNodeLabels[i])) {
                labelCombination |= 1L << i;
            }
        }

        return labelCombination;
    }

    private NodeLabelHistogram() {}
}
