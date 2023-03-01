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
package org.neo4j.gds.ml.linkmodels;

import com.carrotsearch.hppc.AbstractIterator;
import org.neo4j.gds.core.utils.queue.BoundedLongLongPriorityQueue;

import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ExhaustiveLinkPredictionResult implements LinkPredictionResult {

    private final BoundedLongLongPriorityQueue predictionQueue;
    private final long linksConsidered;

    public ExhaustiveLinkPredictionResult(BoundedLongLongPriorityQueue bestPredictions, long linksConsidered) {
        this.predictionQueue = bestPredictions;
        this.linksConsidered = linksConsidered;
    }

    public int size() {
        return predictionQueue.size();
    }

    public void forEach(BoundedLongLongPriorityQueue.Consumer consumer) {
        predictionQueue.foreach(consumer);
    }

    @Override
    public Stream<PredictedLink> stream() {
        Iterable<PredictedLink> iterable = () -> new AbstractIterator<>() {

            final PrimitiveIterator.OfLong elements1Iter = predictionQueue.elements1().iterator();
            final PrimitiveIterator.OfLong elements2Iter = predictionQueue.elements2().iterator();
            final PrimitiveIterator.OfDouble prioritiesIter = predictionQueue.priorities().iterator();

            @Override
            protected PredictedLink fetch() {
                if (!elements1Iter.hasNext()) {
                    return done();
                }
                return PredictedLink.of(
                    elements1Iter.nextLong(),
                    elements2Iter.nextLong(),
                    prioritiesIter.nextDouble()
                );
            }
        };

        return StreamSupport.stream(iterable.spliterator(), true);
    }

    @Override
    public Map<String, Object> samplingStats() {
        return Map.of(
            "strategy", "exhaustive",
            "linksConsidered", linksConsidered
        );
    }
}
