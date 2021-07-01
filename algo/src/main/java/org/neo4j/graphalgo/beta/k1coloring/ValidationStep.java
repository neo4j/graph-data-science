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
package org.neo4j.graphalgo.beta.k1coloring;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;

final class ValidationStep implements Runnable {

    private final RelationshipIterator graph;
    private final HugeLongArray colors;
    private final BitSet currentNodesToColor;
    private final BitSet nextNodesToColor;
    private final Partition partition;
    private final ProgressTracker progressTracker;

    ValidationStep(
        RelationshipIterator graph,
        HugeLongArray colors,
        BitSet currentNodesToColor,
        BitSet nextNodesToColor,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.colors = colors;
        this.currentNodesToColor = currentNodesToColor;
        this.nextNodesToColor = nextNodesToColor;
        this.partition = partition;
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        partition.consume(nodeId -> {
            if (currentNodesToColor.get(nodeId)) {
                graph.forEachRelationship(nodeId, (source, target) -> {
                    if (
                        source != target &&
                        colors.get(source) == colors.get(target) &&
                        !nextNodesToColor.get(target)
                    ) {
                        nextNodesToColor.set(source);
                        return false;
                    }

                    return true;
                });

                progressTracker.logProgress();
            }
        });
    }
}
