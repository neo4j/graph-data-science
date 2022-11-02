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
package org.neo4j.gds.embeddings.hashgnn;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class HashGNNCompanionTest {

    @Test
    void shouldComputeHashesFromTriple() {
        int EMBEDDING_DIMENSION = 10;

        var rng = new SplittableRandom();
        int c = rng.nextInt(2, 100);
        int a = rng.nextInt(c - 1) + 1;
        int b = rng.nextInt(c - 1) + 1;

        var hashTriple = ImmutableHashTriple.of(a, b, c);
        var hashes = HashGNNCompanion.HashTriple.computeHashesFromTriple(EMBEDDING_DIMENSION, hashTriple);

        assertThat(hashes.length).isEqualTo(EMBEDDING_DIMENSION);
        assertThat(hashes).containsAnyOf(IntStream.range(0, c).toArray());
    }

    @Test
    void shouldHashArgMin() {
        var rng = new SplittableRandom();

        var bitSet = new BitSet(10);
        bitSet.set(3);
        bitSet.set(9);

        var hashes = IntStream.generate(() -> rng.nextInt(0, Integer.MAX_VALUE)).limit(10).toArray();
        var minArgMin = new HashGNN.MinAndArgmin();

        HashGNNCompanion.hashArgMin(bitSet, hashes, minArgMin);

        assertThat(minArgMin.min).isEqualTo(Math.min(hashes[3], hashes[9]));
        assertThat(minArgMin.argMin).isEqualTo(hashes[3] <= hashes[9] ? 3 : 9);
    }

}
