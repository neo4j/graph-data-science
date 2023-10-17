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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class RandomWalkTask extends GeneralRandomWalkTask {

    private final BlockingQueue<long[]> walks;
    private final long[][] buffer;
    private int bufferLength;
    private final TerminationFlag terminationFlag;


    public RandomWalkTask(
        NextNodeSupplier nextNodeSupplier,
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
        RandomWalkBaseConfig config,
        BlockingQueue<long[]> walks,
        Graph graph,
        long randomSeed,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
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
        this.walks = walks;
        this.buffer = new long[1000][];
        Function<long[], Boolean> func = path -> {
            buffer[bufferLength++] = path;
            if (bufferLength == buffer.length) {
                var shouldStop = flushBuffer(bufferLength);
                bufferLength = 0;
                return shouldStop;

            }
            return true;
        };
        withPathConsumer(func);
    }

    @Override
    public void run() {
        super.run();
        flushBuffer(bufferLength);
    }

    // returns false if execution should be stopped, otherwise true
    private boolean flushBuffer(int bufferLength) {
        bufferLength = Math.min(bufferLength, this.buffer.length);

        int i = 0;
        while (i < bufferLength && terminationFlag.running()) {
            try {
                // allow termination to occur if queue is full
                if (walks.offer(this.buffer[i], 100, TimeUnit.MILLISECONDS)) {
                    i++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return terminationFlag.running();
    }
}
