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
package org.neo4j.gds.maxflow;

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.termination.TerminationFlag;

public class ParallelGapDetector implements GapDetector {

    private final HugeLongArray counts;
    private final Concurrency concurrency;
    private final long nodeCount;
    private final HugeLongArray label;

    public ParallelGapDetector(HugeLongArray label, long nodeCount, Concurrency concurrency) {
        this.label = label;
        this.nodeCount = nodeCount;
        this.counts = HugeLongArray.newArray(nodeCount + 2);
        this.concurrency = concurrency;
    }

    @Override
    public void resetCounts() {
        counts.setAll(v -> 0);
        for (int i = 0; i < nodeCount; ++i) {
            counts.addTo(label.get(i), 1);
        }
    }

    @Override
    public void relabel(long gap) {
        ParallelUtil.parallelForEachNode(
            nodeCount,
            concurrency,
            TerminationFlag.RUNNING_TRUE,
            (v) -> {
                if (label.get(v) >= gap) {
                    label.set(v, nodeCount);
                }
            }
        );
    }

    @Override
    public boolean moveFrom(long node, long from, long to) {
        counts.addTo(from, -1);
        counts.addTo(to, 1);
        return counts.get(from) == 0;
    }
}
