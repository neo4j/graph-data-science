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
package org.neo4j.graphalgo.impl.similarity;

import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.proc.SimilarityProc;

import static org.neo4j.graphalgo.impl.similarity.SimilarityStreamGenerator.computeSimilarityForSourceIndex;

class TopKTask<T> implements Runnable {
    private final int batchSize;
    private final int taskOffset;
    private final int multiplier;
    private final int length;
    private final T[] ids;
    private final double similiarityCutoff;
    private final SimilarityComputer<T> computer;
    private RleDecoder decoder;
    private final TopKConsumer<SimilarityResult>[] topKConsumers;

    TopKTask(int batchSize, int taskOffset, int multiplier, int length, T[] ids, double similiarityCutoff, int topK, SimilarityComputer<T> computer, RleDecoder decoder) {
        this.batchSize = batchSize;
        this.taskOffset = taskOffset;
        this.multiplier = multiplier;
        this.length = length;
        this.ids = ids;
        this.similiarityCutoff = similiarityCutoff;
        this.computer = computer;
        this.decoder = decoder;
        topKConsumers = SimilarityProc.initializeTopKConsumers(length, topK);
    }

    @Override
    public void run() {
        SimilarityConsumer consumer = assignSimilarityPairs(topKConsumers);

        for (int offset = 0; offset < batchSize; offset++) {
            int sourceId = taskOffset * multiplier + offset;
            if (sourceId < length) {

                computeSimilarityForSourceIndex(sourceId, ids, length, similiarityCutoff, consumer, computer, decoder);
            }
        }
    }

    void mergeInto(TopKConsumer<SimilarityResult>[] target) {
        for (int i = 0; i < target.length; i++) {
            target[i].accept(topKConsumers[i]);
        }
    }

    public static SimilarityConsumer assignSimilarityPairs(TopKConsumer<SimilarityResult>[] topKConsumers) {
        return (s, t, result) -> {
            topKConsumers[result.reversed ? t : s].accept(result);

            if (result.bidirectional) {
                SimilarityResult reverse = result.reverse();
                topKConsumers[reverse.reversed ? t : s].accept(reverse);
            }
        };
    }
}
