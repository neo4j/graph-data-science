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
package org.neo4j.gds.cliqueCounting;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Long.max;


class CliqueCount extends ConcurrentHashMap<Integer, BigInteger> {
    long[] toLongArray() { //starting at 3 (triangles)
        var maxSize = this.size() == 0 ? 0 : Collections.max(this.keySet());
//        var maxSize = this.size() == 0 ? 0 : this.size() + 2; //we dont store 1- and 2-cliques anymore.
        int newSize = (int) max(maxSize - 2, 0);
        long[] array = new long[newSize];
        for (int i = 0; i < array.length; i++) {
            array[i] = this.getOrDefault(i + 3, BigInteger.ZERO).longValueExact(); //throws if to big
        }
        return array;
    }
}

class ListCliqueCount {
    private BigInteger[] data;
//    private ReentrantLock lock;

    public ListCliqueCount() {
        this.data = new BigInteger[0];
//        this.lock = new ReentrantLock();
    }

    public void add(int requiredNodesCount, int optionalNodesCount) {
        synchronized (this) { //possibly stupid
            //binomial(optionalNodesCount, selectedOptionalNodesCount) = numerator / denominator
            //lock entire?

            //grow if needed
            var oldLength = this.data.length;
            var newLength = requiredNodesCount + optionalNodesCount - 2;
            if (oldLength < newLength) {
                data = Arrays.copyOf(this.data, newLength); //Might cause concurrency problems if not locked :/
                for (int i = oldLength; i < newLength; i++) {
                    data[i] = BigInteger.valueOf(0L);
                }
            }

            //0 optional nodes
            BigInteger numerator = BigInteger.ONE;
            BigInteger denominator = BigInteger.ONE;
            var cliqueSizeIdx = requiredNodesCount - 3;
            var count = numerator.divide(denominator);
            if (cliqueSizeIdx >= 0) {
                data[cliqueSizeIdx] = data[cliqueSizeIdx].add(count);
            }

            //>=1 optional node(s)
            for (var selectedOptionalNodesCount = 1; selectedOptionalNodesCount <= optionalNodesCount; selectedOptionalNodesCount++) {
                numerator = numerator.multiply(BigInteger.valueOf(optionalNodesCount - selectedOptionalNodesCount + 1));
                denominator = denominator.multiply(BigInteger.valueOf(selectedOptionalNodesCount));
                cliqueSizeIdx = requiredNodesCount + selectedOptionalNodesCount - 3;
                if (cliqueSizeIdx < 0) {
                    continue;
                }
                count = numerator.divide(denominator);
                data[cliqueSizeIdx] = data[cliqueSizeIdx].add(count);
            }
            //unlock
        }
    }

    public static void add(int requiredNodesCount, int optionalNodesCount, Iterable<ListCliqueCount> cliqueCounts) {
        {
            //fixme: Not thread-safe
            //binomial(optionalNodesCount, selectedOptionalNodesCount) = numerator / denominator

            //grow if needed
            var newLength = requiredNodesCount + optionalNodesCount - 2;
            for (ListCliqueCount cliqueCount : cliqueCounts) {
//                cliqueCount.lock.lock();
                var oldLength = cliqueCount.data.length;
                if (oldLength < newLength) {
                    cliqueCount.data = Arrays.copyOf(cliqueCount.data, newLength);
                    for (int i = oldLength; i < newLength; i++) {
                        cliqueCount.data[i] = BigInteger.valueOf(0L);
                    }
                }
//                cliqueCount.lock.unlock();
            }

            //0 optional nodes
            BigInteger numerator = BigInteger.ONE;
            BigInteger denominator = BigInteger.ONE;
            var cliqueSizeIdx = requiredNodesCount - 3;
            var count = numerator.divide(denominator);
            if (cliqueSizeIdx >= 0) {
                for (ListCliqueCount cliqueCount : cliqueCounts) {
//                    cliqueCount.lock.lock();
                    cliqueCount.data[cliqueSizeIdx] = cliqueCount.data[cliqueSizeIdx].add(count);
//                    cliqueCount.lock.unlock();
                }
            }

            //>=1 optional node(s)
            for (var selectedOptionalNodesCount = 1; selectedOptionalNodesCount <= optionalNodesCount; selectedOptionalNodesCount++) {
                numerator = numerator.multiply(BigInteger.valueOf(optionalNodesCount - selectedOptionalNodesCount + 1));
                denominator = denominator.multiply(BigInteger.valueOf(selectedOptionalNodesCount));
                cliqueSizeIdx = requiredNodesCount + selectedOptionalNodesCount - 3;
                if (cliqueSizeIdx < 0) {
                    continue;
                }
                count = numerator.divide(denominator);
                for (ListCliqueCount cliqueCount : cliqueCounts) {
//                    cliqueCount.lock.lock();
                    cliqueCount.data[cliqueSizeIdx] = cliqueCount.data[cliqueSizeIdx].add(count);
//                    cliqueCount.lock.unlock();
                }
            }
            //unlock
        }
    }

    public long[] toLongArray() {
        return Arrays.stream(this.data).mapToLong(BigInteger::longValueExact).toArray();
    }
}
