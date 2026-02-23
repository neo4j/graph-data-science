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
package org.neo4j.gds.paths.yens;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;

public final class RelationshipPruningTask implements Runnable {
    private final Graph graph;
    private final Long sourceNode;
    private final LongToDoubleFunction sourceCost;
    private final LongToDoubleFunction targetCost;
    private final LongPredicate nodeIncluded;
    private final double cutoff;
    private final Partition partition;
    private final RelationshipsBuilder relationshipsBuilder;
    private final ProgressTracker progressTracker;

    RelationshipPruningTask(
        Graph graph,
        long sourceNode,
        LongToDoubleFunction sourceCost,
        LongToDoubleFunction targetCost,
        LongPredicate nodeIncluded,
        double cutoff,
        Partition partition,
        RelationshipsBuilder relationshipsBuilder,
        ProgressTracker progressTracker)
    {
        this.graph = graph;
        this.sourceNode = sourceNode;
        this.sourceCost = sourceCost;
        this.targetCost = targetCost;
        this.cutoff = cutoff;
        this.nodeIncluded = nodeIncluded;
        this.partition = partition;
        this.relationshipsBuilder = relationshipsBuilder;
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        long start = partition.startNode();
        long finish = start + partition.nodeCount();
        for (long n = start; n < finish; n++) {
            if (nodeIncluded.test(n)) {
                double sCost = sourceCost.applyAsDouble(n);
                graph.forEachRelationship(n, 1.0, (source, target, weight) -> {
                    progressTracker.logProgress(1);
                    if (nodeIncluded.test(target) && (target != sourceNode) && (cutoff >= weight + sCost + targetCost.applyAsDouble(target))) {
                        relationshipsBuilder.add(source, target, weight);
                    }
                    return true;
                });
            }
        }
    }
}
