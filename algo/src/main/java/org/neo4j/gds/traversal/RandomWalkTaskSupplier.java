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
package org.neo4j.gds.traversal;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;

import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

class RandomWalkTaskSupplier implements Supplier<RandomWalkTask> {
    private final Supplier<Graph> graphSupplier;
    private final NextNodeSupplier nextNodeSupplier;
    private final RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier;
    private final BlockingQueue<long[]> walks;
    private final int walksPerNode;
    private final int walkLength;
    private final double returnFactor;
    private final double inOutFactor;
    private final long randomSeed;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    RandomWalkTaskSupplier(
        Supplier<Graph> graphSupplier,
        NextNodeSupplier nextNodeSupplier,
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
        BlockingQueue<long[]> walks,
        int walksPerNode,
        int walkLength,
        double returnFactor,
        double inOutFactor,
        long randomSeed,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.graphSupplier = graphSupplier;
        this.nextNodeSupplier = nextNodeSupplier;
        this.cumulativeWeightSupplier = cumulativeWeightSupplier;
        this.walks = walks;
        this.walksPerNode = walksPerNode;
        this.walkLength = walkLength;
        this.returnFactor = returnFactor;
        this.inOutFactor = inOutFactor;
        this.randomSeed = randomSeed;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public RandomWalkTask get() {
        return new RandomWalkTask(
            graphSupplier.get(),
            nextNodeSupplier,
            cumulativeWeightSupplier,
            walks,
            walksPerNode,
            walkLength,
            returnFactor,
            inOutFactor,
            randomSeed,
            progressTracker,
            terminationFlag
        );
    }
}
