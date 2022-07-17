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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.core.Aggregation;

import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class AdjacencyPackerTest {

    @SuppressForbidden(reason = "Just wanna test some stuff")
    @Test
    void name() {
        var random = new Random();
        var seed = random.nextLong();
        random.setSeed(seed);
        var values = random.longs(256, 0, 1L << 50).toArray();
        Arrays.sort(values);
        var originalValues = Arrays.copyOf(values, values.length);
        var uncompressedSize = originalValues.length * Long.BYTES;

        var compressed = AdjacencyPacker.compress(values, 0, values.length, AdjacencyPacker.DELTA);
        var newRequiredBytes = compressed.bytesUsed();
        System.out.printf(
            Locale.ENGLISH,
            "new compressed = %d ratio = %.2f%n",
            newRequiredBytes,
            (double) newRequiredBytes / uncompressedSize
        );

        var decompressed = AdjacencyPacker.decompressAndPrefixSum(compressed);
        assertThat(decompressed).containsExactly(originalValues);

        values = Arrays.copyOf(originalValues, originalValues.length);
        AdjacencyCompression.deltaEncodeSortedValues(values, 0, values.length, Aggregation.NONE);
        var compressedBuffer = new byte[values.length * Long.BYTES];
        int requiredBytes = AdjacencyCompression.compress(values, compressedBuffer, values.length);
        System.out.printf(
            Locale.ENGLISH,
            "dvl compressed = %d ratio = %.2f%n",
            requiredBytes,
            (double) requiredBytes / uncompressedSize
        );

        assertThat(newRequiredBytes)
            .as("new compressed should be less than dvl compressed, seed = %d", seed)
            .isLessThanOrEqualTo(requiredBytes);
    }

}
