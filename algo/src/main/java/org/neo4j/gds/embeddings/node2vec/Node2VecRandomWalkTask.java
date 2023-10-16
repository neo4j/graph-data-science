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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;
import org.neo4j.gds.traversal.GeneralRandomWalkTask;
import org.neo4j.gds.traversal.NextNodeSupplier;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

final class Node2VecRandomWalkTask extends GeneralRandomWalkTask {
    
    private int walks;
    private final TerminationFlag terminationFlag;
    private  int  maxWalkLength;
    private long maxIndex;

    public Node2VecRandomWalkTask(
        NextNodeSupplier nextNodeSupplier,
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
        RandomWalkBaseConfig config,
        Graph graph,
        long randomSeed,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        AtomicLong walkIndex,
        CompressedRandomWalks compressedRandomWalks,
        RandomWalkProbabilities.Builder randomWalkProbabilitiesBuilder
    ) {
        super(
            nextNodeSupplier,
            cumulativeWeightSupplier,
            config,
            graph,
            randomSeed,
            progressTracker
        );

        this.terminationFlag = terminationFlag;

        Function<long[], Boolean> func = path -> {
            var index = walkIndex.getAndIncrement(); //perhaps we can also use a buffer to minimize walkIndex atomic operations
            maxIndex = index;
            randomWalkProbabilitiesBuilder.registerWalk(path);
            compressedRandomWalks.add(index, path);
            maxWalkLength = Math.max(path.length, maxWalkLength);
            if (walks++ == 1000) { //this is just to get the same
                walks = 0;
                return this.terminationFlag.running();
            }
            return true;
             };

        withPathConsumer(func);

    }

    public int maxWalkLength(){
        return  maxWalkLength;
    }

    public long maxIndex() {
        return maxIndex;
    }
}
