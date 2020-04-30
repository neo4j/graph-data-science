/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.embedding;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.graphalgo.impl.embedding.RandomProjection;
import org.neo4j.logging.Log;

public class RandomProjectionFactory<CONFIG extends RandomProjectionBaseConfig> extends AlgorithmFactory<RandomProjection, CONFIG> {

    @Override
    public RandomProjection build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, Log log
    ) {
        // TODO logging
        var progressLogger = new BatchingProgressLogger(log, 1, "RandomProjection", configuration.concurrency());

        return new RandomProjection(
            graph,
            configuration.embeddingDimension(),
            configuration.sparsity(),
            configuration.maxIterations(),
            configuration.iterationWeights(),
            configuration.normalizationStrength(),
            configuration.normalizeL2(),
            configuration.seed(),
            configuration.concurrency(),
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        throw new MemoryEstimationNotImplementedException();
    }
}
