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
import com.carrotsearch.hppc.BitSetIterator;
import org.apache.commons.math3.primes.Primes;
import org.neo4j.gds.annotation.ValueClass;

import java.util.SplittableRandom;

public class HashGNNCompanion {
    private HashGNNCompanion() {}


   static void hashArgMin(BitSet bitSet, int[] hashes, HashGNN.MinAndArgmin minAndArgmin) {
       int argMin = -1;
       int minHash = Integer.MAX_VALUE;
       var iterator = bitSet.iterator();
       var bit = iterator.nextSetBit();
       while (bit != BitSetIterator.NO_MORE) {
           int hash = hashes[bit];

           if (hash < minHash) {
               minHash = hash;
               argMin = bit;
           }

           bit = iterator.nextSetBit();
       }

       minAndArgmin.min = minHash;
       minAndArgmin.argMin = argMin;
   }

    @ValueClass
   interface HashTriple {
        /*
        The values a, b and c represent parameters of the hash function: h(x) = x * a + b mod c,
        where 0 < a, b < c and c is a prime number.
        */

       int a();

       int b();

       int c();

       static HashTriple generate(SplittableRandom rng) {
           int c = Primes.nextPrime(rng.nextInt(1, Integer.MAX_VALUE));
           return generate(rng, c);
       }

       static HashTriple generate(SplittableRandom rng, int c) {
           int a = rng.nextInt(1, c);
           int b = rng.nextInt(1, c);
           return ImmutableHashTriple.of(a, b, c);
       }

        static int[] computeHashesFromTriple(int ambientDimension, HashTriple hashTriple) {
            var output = new int[ambientDimension];
            for (int i = 0; i < ambientDimension; i++) {
                // without cast to long, we can easily overflow Integer.MAX_VALUE which can to negative hash values
                output[i] = Math.toIntExact(((long) i * hashTriple.a() + hashTriple.b()) % hashTriple.c());
            }

            return output;
        }
   }
}
