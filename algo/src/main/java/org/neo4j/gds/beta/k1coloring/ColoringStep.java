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
package org.neo4j.gds.beta.k1coloring;

import com.carrotsearch.hppc.BitSet;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public final class ColoringStep implements Runnable {

    public static final int INITIAL_FORBIDDEN_COLORS = 1000;
    
    private final RelationshipIterator graph;
    private final HugeLongArray colors;
    private final BitSet nodesToColor;
    private final BitSet forbiddenColors;
    private final Partition partition;
    private final ProgressTracker progressTracker;
    private final long[] resetMask;

    public ColoringStep(
        RelationshipIterator graph,
        HugeLongArray colors,
        BitSet nodesToColor,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.colors = colors;
        this.nodesToColor = nodesToColor;
        this.partition = partition;
        this.forbiddenColors = new BitSet(INITIAL_FORBIDDEN_COLORS);
        this.resetMask = new long[INITIAL_FORBIDDEN_COLORS];
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        var coloredNodes = new MutableLong(0);
        partition.consume(nodeId -> {
            if (nodesToColor.get(nodeId)) {
                coloredNodes.increment();
                resetForbiddenColors();

                graph.forEachRelationship(nodeId, (s, target) -> {
                    if (s != target) {
                        forbiddenColors.set(colors.get(target));
                    }
                    return true;
                });

                long nextColor = 0;
                while (forbiddenColors.get(nextColor)) {
                    nextColor++;
                }

                colors.set(nodeId, nextColor);
            }
        });
        progressTracker.logProgress(coloredNodes.longValue());
    }

    private void resetForbiddenColors() {
        for (int i = 0; i <= forbiddenColors.bits.length; i += resetMask.length) {
            System.arraycopy(resetMask, 0, forbiddenColors.bits, i, Math.min(forbiddenColors.bits.length -i, INITIAL_FORBIDDEN_COLORS));
            forbiddenColors.wlen = 0;
        }
    }
}
