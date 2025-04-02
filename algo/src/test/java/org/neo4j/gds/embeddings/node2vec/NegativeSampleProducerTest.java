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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

class NegativeSampleProducerTest {

    @Test
    void shouldProduceSamplesAccordingToNodeDistribution() {

        var builder = new RandomWalkProbabilities.Builder(
            2,
            new Concurrency(4),
            0.001,
            0.75
        );

        builder
            .registerWalk(new long[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

        builder.registerWalk(new long[]{1});

        RandomWalkProbabilities probabilityComputer = builder.build();
        var sampler = new NegativeSampleProducer(probabilityComputer.negativeSamplingDistribution(),0);

        var distribution  = new HashMap<Long,Integer>();
        int SAMPLES  = 1300;
        for (int i=0;i<SAMPLES;++i){
            var next = sampler.next();
            distribution.put(next,  distribution.getOrDefault(next,0) + 1);
        }

        // We samples nodes with a probability of their number of occurrences^0.75 (16^0.75=12, 1^0.75=1)
        assertThat(distribution.get(1L).doubleValue() / distribution.get(0L)).isCloseTo(1.0/12, Offset.offset(0.1));
    }

    @Test
    void shouldProduceDifferentlyIfSeeded() {

        var sampler1 = new NegativeSampleProducer(HugeLongArray.of(16,18),0);
        var sampler2 = new NegativeSampleProducer(HugeLongArray.of(16,18),1);

        var distribution1 = new HashMap<Long,Integer>();
        var distribution2 = new HashMap<Long,Integer>();

        int SAMPLES = 1300;
        for (int i=0;i<SAMPLES;++i){
            var next1 = sampler1.next();
            distribution1.put(next1,  distribution1.getOrDefault(next1,0) + 1);
            //
            var next2 = sampler2.next();
            distribution2.put(next2,  distribution1.getOrDefault(next2,0) + 1);
        }

        assertThat(distribution1.get(0L)).isNotEqualTo(distribution2.get(0L));
        assertThat(distribution1.get(1L)).isNotEqualTo(distribution2.get(1L));

    }

}
