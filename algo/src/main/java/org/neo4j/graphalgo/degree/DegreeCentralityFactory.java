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
package org.neo4j.graphalgo.degree;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.logging.Log;

public class DegreeCentralityFactory<CONFIG extends DegreeCentralityConfig> implements AlgorithmFactory<DegreeCentrality, CONFIG> {

    @Override
    public DegreeCentrality build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, Log log, ProgressEventTracker eventTracker
    ) {
        var progressLogger = new BatchingProgressLogger(
            log,
            graph.nodeCount(),
            "DegreeCentrality",
            configuration.concurrency(),
            eventTracker
        );
        return new DegreeCentrality(graph, Pools.DEFAULT, configuration, progressLogger, tracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(DegreeCentrality.class);
        if (configuration.cacheDegrees() || configuration.relationshipWeightProperty() != null) {
            return builder
                .perNode("degree cache", HugeDoubleArray::memoryEstimation)
                .build();
        }
        return builder.build();
    }
}
