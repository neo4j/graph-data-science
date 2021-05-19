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
package org.neo4j.gds.ml.core.batch;

import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.queue.BoundedLongPriorityQueue;

import java.util.Random;
import java.util.function.LongPredicate;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/*
 * Weighted Reservoir Sampling based on Algorithm A-Res:
 * https://en.wikipedia.org/wiki/Reservoir_sampling#Algorithm_A-Res
 */
public class WeightedUniformSampler {

    // Used in the denominators in order to avoid division by zero.
    private static final double EPSILON = 1e-10;
    private final Random random;

    public WeightedUniformSampler(long randomSeed) {
        this.random = new Random(randomSeed);
    }

    public LongStream sample(Stream<RelationshipCursor> input, long inputSize, int numberOfSamples) {
        return sample(input, inputSize, numberOfSamples, v -> true);
    }

    public LongStream sample(Stream<RelationshipCursor> input, long inputSize, int numberOfSamples, LongPredicate includeNode) {
        if (numberOfSamples == 0) {
            return LongStream.empty();
        }

        if (numberOfSamples >= inputSize) {
            return input.mapToLong(RelationshipCursor::targetId);
        }

        var reservoir = BoundedLongPriorityQueue.max(numberOfSamples);
        var inputIterator = input.iterator();

        while (inputIterator.hasNext()) {
            processRelationship(reservoir, inputIterator.next(), includeNode);
        }

        return reservoir.elements();
    }

    private void processRelationship(
        BoundedLongPriorityQueue reservoir,
        RelationshipCursor relationshipCursor,
        LongPredicate includeNode
    ) {
        var node =  relationshipCursor.targetId();
        if (includeNode.test(node)) {
            // Higher weights should be more likely to be sampled
            var priority = Math.pow(random.nextDouble(), 1 / (relationshipCursor.property() + EPSILON));
            reservoir.offer(node, priority);
        }
    }
}
