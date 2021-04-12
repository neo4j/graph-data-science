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

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;

public class PositiveSampleProducer {

    private static final int FILTERED_NODE_MARKER = -2;

    private final Iterator<long[]> walks;
    private final HugeDoubleArray centerNodeProbabilities;
    private final int prefixWindowSize;
    private final int postfixWindowSize;
    private final ProgressLogger progressLogger;
    private long[] currentWalk;
    private int centerWordIndex;
    private long currentCenterWord;
    private int contextWordIndex;
    private int currentWindowStart;
    private int currentWindowEnd;

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
        long[] walk = walks.next();
        int filteredWalkLength = filter(walk);

        while (walks.hasNext() && filteredWalkLength < 2) {
            walk = walks.next();
            filteredWalkLength = filter(walk);
            progressLogger.logProgress();
        }

        if (filteredWalkLength >= 2) {
            progressLogger.logProgress();
            this.currentWalk = walk;
            centerWordIndex = -1;
            return nextCenterWord();
        }

        return false;
    }

    private boolean nextCenterWord() {
        centerWordIndex++;

        if (centerWordIndex >= currentWalk.length || currentWalk[centerWordIndex] == -1) {
            return nextWalk();
        } else if (currentWalk[centerWordIndex] == -2) {
            return nextCenterWord();
        } else {
            currentCenterWord = currentWalk[centerWordIndex];

            setContextBoundaries();
            contextWordIndex = currentWindowStart - 1;
            return nextContextWord();
        }
    }

    private boolean nextContextWord() {
        if (currentWalk.length == 0) {
            return nextCenterWord();
        }

        contextWordIndex++;

        if(contextWordIndex <= currentWindowEnd && contextWordIndex != centerWordIndex && currentWalk[contextWordIndex] >= 0) {
            return true;
        } else if (contextWordIndex > currentWindowEnd) {
            return nextCenterWord();
        }

        return nextContextWord();
    }

    private int filter(long[] walk) {
        int filteredWalkLength = 0;
        for (int i = 0; i < walk.length; i++) {
            if (walk[i] >= 0 && shouldPickNode(walk[i])) {
                filteredWalkLength++;
            } else if (walk[i] >= 0) {
                walk[i] = FILTERED_NODE_MARKER;
            }
        }

        return filteredWalkLength;
    }

    private boolean shouldPickNode(long nodeId) {
        return ThreadLocalRandom.current().nextDouble(0, 1) < centerNodeProbabilities.get(nodeId);
    }

    // We need to adjust the window size for a given center word to ignore filtered nodes that might occur in the window
    private void setContextBoundaries() {
        var currentPrefixSize = prefixWindowSize;
        currentWindowStart = centerWordIndex;
        while(currentPrefixSize > 0 && currentWindowStart > 0) {
            currentWindowStart--;
            if (currentWindowStart >= 0 && currentWalk[currentWindowStart] > 0) {
                currentPrefixSize--;
            }
        };

        var currentPostfixSize = postfixWindowSize;
        currentWindowEnd = centerWordIndex ;
        while (currentPostfixSize > 0 && currentWindowEnd < currentWalk.length - 1 && currentWalk[currentWindowEnd] != -1) {
            currentWindowEnd++;
            if (currentWalk[currentWindowEnd] > 0) {
                currentPostfixSize--;
            }
        }
    }
}
