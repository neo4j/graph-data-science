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
package org.neo4j.graphalgo.impl.coloring;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

public final class ColoringStep implements Runnable {

    private final RelationshipIterator graph;
    private final Direction direction;
    private final HugeLongArray colors;
    private final BitSet nodesToColor;
    private final long offset;
    private final long batchEnd;

    private final BitSet forbiddenColors;
    private final long[] resetMask;

    public ColoringStep(
        RelationshipIterator graph,
        Direction direction,
        HugeLongArray colors,
        BitSet nodesToColor,
        long nodeCount,
        long offset,
        long batchSize
    ) {
        this.graph = graph;
        this.direction = direction;
        this.colors = colors;
        this.nodesToColor = nodesToColor;
        this.offset = offset;
        this.batchEnd = Math.min(offset + batchSize, nodeCount);
        this.forbiddenColors = new BitSet(1000);
        this.resetMask = new long[forbiddenColors.bits.length];
        Arrays.fill(resetMask, 0);
    }

    @Override
    public void run() {
        for (long nodeId = offset; nodeId <= batchEnd; nodeId++) {
            if (nodesToColor.get(nodeId)) {
                resetForbiddenColors();

                graph.forEachRelationship(nodeId, direction, (s, target) -> {
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
        }
    }

    private void resetForbiddenColors() {
        System.arraycopy(resetMask, 0, forbiddenColors.bits, 0, forbiddenColors.bits.length);
        forbiddenColors.wlen = 0;
    }
}
