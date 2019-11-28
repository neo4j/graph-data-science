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

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;

public class K1ColoringFactory extends AlgorithmFactory<K1Coloring, ProcedureConfiguration> {

    public static final int DEFAULT_ITERATIONS = 10;
    public static final Direction DEFAULT_DIRECTION = Direction.OUTGOING;

    @Override
    public K1Coloring build(
        final Graph graph,
        final ProcedureConfiguration configuration,
        final AllocationTracker tracker,
        final Log log
    ) {
        int concurrency = configuration.concurrency();
        int batchSize = configuration.getBatchSize();

        int maxIterations = configuration.getIterations(DEFAULT_ITERATIONS);
        Direction direction = configuration.getDirection(DEFAULT_DIRECTION);

        return new K1Coloring(
            graph,
            direction,
            maxIterations,
            batchSize,
            concurrency,
            Pools.DEFAULT,
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(K1Coloring.class)
            .perNode("colors", HugeLongArray::memoryEstimation)
            .perNode("nodesToColor", MemoryUsage::sizeOfBitset)
            .perThread("coloring", MemoryEstimations.builder()
                .field("coloringStep", ColoringStep.class)
                .perNode("forbiddenColors", MemoryUsage::sizeOfBitset)
                .build())
            .build();
    }
}
