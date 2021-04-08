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

import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;

public class PositiveSampleProducer {

    private final Iterator<long[]> walks;
    private final HugeDoubleArray centerNodeProbabilities;
    private final int prefixWindowSize;
    private final int postfixWindowSize;
    private long[] currentWalk;
    private long currentCenterWord;
    private final ProgressLogger progressLogger;
    private int centerWordIndex;
    private int contextWordIndex;

    PositiveSampleProducer(
        Iterator<long[]> walks,
        HugeDoubleArray centerNodeProbabilities,
        int windowSize,
        ProgressLogger progressLogger
    ) {
        this.walks = walks;
        this.progressLogger = progressLogger;
        this.centerNodeProbabilities = centerNodeProbabilities;

        prefixWindowSize = ceilDiv(windowSize - 1, 2);
        postfixWindowSize = (windowSize - 1) / 2;

        this.currentWalk = new long[0];
        this.centerWordIndex = -1;
        this.contextWordIndex = 1;
    }

    public boolean next(long[] buffer) {
        if (nextContextWord()) {
            buffer[0] = currentCenterWord;
            buffer[1] = currentWalk[contextWordIndex];
            return true;
        }

        return false;
    }

    private boolean nextWalk() {
        if (!walks.hasNext()) {
            return false;
        }
        long[] walk = filter(walks.next());

        while (walks.hasNext() && walk.length < 2) {
            walk = filter(walks.next());
            progressLogger.logProgress();
        }

        if (walk.length >= 2) {
            progressLogger.logProgress();
            this.currentWalk = walk;
            centerWordIndex = -1;
            return nextCenterWord();
        }

        return false;
    }

    private boolean nextCenterWord() {
        centerWordIndex++;

        if (centerWordIndex < currentWalk.length) {
            currentCenterWord = currentWalk[centerWordIndex];
            contextWordIndex = Math.max(0, centerWordIndex - prefixWindowSize) - 1;
            return nextContextWord();
        } else {
            return nextWalk();
        }
    }

    private boolean nextContextWord() {
        contextWordIndex++;
        if (centerWordIndex == contextWordIndex) {
            contextWordIndex++;
        }

        if (contextWordIndex >= Math.min(centerWordIndex + postfixWindowSize + 1, currentWalk.length)) {
            return nextCenterWord();
        }

        return true;
    }

    private long[] filter(long[] walk) {
        return Arrays.stream(walk).filter(this::shouldPickNode).toArray();
    }

    private boolean shouldPickNode(long nodeId) {
        return ThreadLocalRandom.current().nextDouble(0, 1) < centerNodeProbabilities.get(nodeId);
    }
}
