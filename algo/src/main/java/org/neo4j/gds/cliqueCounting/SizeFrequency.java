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
import java.util.List;

final class SizeFrequency {
    BigInteger[] data;

    SizeFrequency() {
        this.data = new BigInteger[0];
    }

    public void growIfNeeded(int newLength) {
        var oldLength = this.data.length;
        if (oldLength < newLength) {
            this.data = Arrays.copyOf(this.data, newLength);
            for (int i = oldLength; i < newLength; i++) {
                this.data[i] = BigInteger.valueOf(0L);
            }
        }
    }

    protected void add(int requiredNodesCount, int optionalNodesCount) {
        add(requiredNodesCount, optionalNodesCount, List.of(this));
    }

    protected static void add(
        int requiredNodesCount,
        int optionalNodesCount,
        Iterable<SizeFrequency> sizeFrequencyRefs
    ) {
        {
            //grow if needed
            var newLength = requiredNodesCount + optionalNodesCount - 2;
            for (SizeFrequency sizeFrequency : sizeFrequencyRefs) {
                sizeFrequency.growIfNeeded(newLength);
            }

            //0 optional nodes
            BigInteger numerator = BigInteger.ONE;
            BigInteger denominator = BigInteger.ONE;
            var cliqueSizeIdx = requiredNodesCount - 3;
            var count = numerator.divide(denominator);
            if (cliqueSizeIdx >= 0) {
                for (SizeFrequency sizeFrequency : sizeFrequencyRefs) {
                    sizeFrequency.data[cliqueSizeIdx] = sizeFrequency.data[cliqueSizeIdx].add(count);
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
                for (SizeFrequency sizeFrequency : sizeFrequencyRefs) {
                    //binomial(optionalNodesCount, selectedOptionalNodesCount) = numerator / denominator
                    sizeFrequency.data[cliqueSizeIdx] = sizeFrequency.data[cliqueSizeIdx].add(count);
                }
            }
        }
    }

    protected void merge(SizeFrequency other) {
        this.growIfNeeded(other.data.length);
        for (int idx = 0; idx < other.data.length; idx++) {
            data[idx] = data[idx].add(other.data[idx]);
        }
    }

    public long[] toLongArray() {
        return Arrays.stream(this.data).mapToLong(BigInteger::longValueExact).toArray();
    }
}
