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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

final class ReroutingSupplier {

    static ReroutingAlgorithm createRerouter(
        Graph graph,
        long sourceId,
        List<Long> terminals,
        BitSet isTerminal,
        HugeLongArray examinationQueue,
        LongAdder indexQueue,
        int concurrency,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {

        if (graph.characteristics().isInverseIndexed() && !graph.characteristics().isUndirected()) {
            return new InverseRerouter(
                graph,
                sourceId,
                isTerminal,
                examinationQueue,
                indexQueue,
                concurrency,
                progressTracker
            );
        } else {
            return new SimpleRerouter(graph, sourceId, terminals, concurrency, progressTracker, terminationFlag);
        }
    }
}
