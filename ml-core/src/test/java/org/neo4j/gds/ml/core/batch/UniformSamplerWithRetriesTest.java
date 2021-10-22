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

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.From;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.eclipse.collections.api.tuple.primitive.IntIntPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

import java.util.SplittableRandom;
import java.util.function.LongPredicate;

import static org.assertj.core.api.Assertions.assertThat;

class UniformSamplerWithRetriesTest {

    @Property(tries = 50)
    void sample(@ForAll @From("n and k") IntIntPair nAndK) {
        int n = nAndK.getOne();
        int k = nAndK.getTwo();
        var rng = new SplittableRandom(19L);

        var sampler = new UniformSamplerWithRetries(rng);
        LongPredicate isOdd = l -> l % 2 != 0;
        var samples= sampler.sample(0, n, n / 2, k, isOdd);

        assertThat(samples).hasSizeGreaterThanOrEqualTo(Math.min(k, n / 2));
        for (long s : samples) {
            assertThat(s).isEven();
        }
    }

    @Provide("n and k")
    final Arbitrary<IntIntPair> nAndK() {
        return Arbitraries.integers().between(2, 100).flatMap(n ->
            Arbitraries.integers().between(1, n - 1).map(k ->
                PrimitiveTuples.pair((int) n, (int) k)));
    }
}
