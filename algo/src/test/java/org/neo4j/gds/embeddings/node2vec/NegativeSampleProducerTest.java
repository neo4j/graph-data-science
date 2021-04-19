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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NegativeSampleProducerTest {

    @Test
    void shouldProduceSamplesAccordingToNodeDistribution() {
        var builder = new RandomWalkProbabilities.Builder(
            2,
            0.001,
            0.75,
            4,
            AllocationTracker.empty()
        );

        builder
            .registerWalk(new long[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
            .registerWalk(new long[]{1});

        RandomWalkProbabilities probabilityComputer = builder.build()  ;

        var sampler = new NegativeSampleProducer(probabilityComputer.contextDistribution());

        Map<Long, Integer> distribution = IntStream
            .range(0, 1300)
            .mapToObj(ignore -> sampler.next())
            .collect(Collectors.toMap(
                Function.identity(),
                ignore -> 1,
                Integer::sum
            ));

        // We samples nodes with a probability of their number of occurrences^0.75 (16^0.75=12, 1^0.75=1)
        assertEquals(1.0 / 12, distribution.get(1L).doubleValue() / distribution.get(0L), 0.1);
    }
}
