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
package org.neo4j.gds.betweenness;

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

interface ForwardTraversor {

    void traverse(long startNodeId);

    void clear();

    static ForwardTraversor create(
        boolean weighted,
        Graph graph,
        HugeObjectArray<LongArrayList> predecessors,
        HugeLongArrayStack backwardNodes,
        HugeLongArray sigma,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        if (weighted) {
            return WeightedForwardTraversor.create(
                graph.concurrentCopy(),
                predecessors,
                backwardNodes,
                sigma,
                terminationFlag,
                progressTracker
            );
        } else {
            return UnweightedForwardTraversor.create(
                graph.concurrentCopy(),
                predecessors,
                backwardNodes,
                sigma,
                terminationFlag,
                progressTracker
            );
        }
    }
}
