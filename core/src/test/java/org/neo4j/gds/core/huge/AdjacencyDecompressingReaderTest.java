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
package org.neo4j.gds.core.huge;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.AdjacencyCompression;
import org.neo4j.gds.core.loading.MutableIntValue;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class AdjacencyDecompressingReaderTest {

    @Test
    void testSkipUntil() {
        var targets = LongStream.range(0, 128).toArray();
        var reader = prepareAdjacencyDecompressingReader(targets);
        var consumed = new MutableIntValue() {};

        var remaining = targets.length;

        // skip until the first target
        var nextTarget = reader.skipUntil(targets[0], remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[1]);
        assertThat(consumed.value).isEqualTo(2);
        remaining -= consumed.value;

        // skip until the end of the first block
        nextTarget = reader.skipUntil(targets[62], remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[63]);
        assertThat(consumed.value).isEqualTo(62);
        remaining -= consumed.value;

        // skip until the second block
        nextTarget = reader.skipUntil(targets[63], remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[64]);
        assertThat(consumed.value).isEqualTo(1);
        remaining -= consumed.value;

        // read to the end
        nextTarget = reader.skipUntil(targets[127], remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[127]);
        assertThat(consumed.value).isEqualTo(63);
        remaining -= consumed.value;

        // read beyond the end
        consumed.value = 0;
        nextTarget = reader.skipUntil(targets[127] + 1, remaining, consumed);
        assertThat(nextTarget).isEqualTo(AdjacencyCursor.NOT_FOUND);
        assertThat(consumed.value).isEqualTo(0);
    }

    @Test
    void testAdvance() {
        var targets = LongStream.range(0, 128).toArray();
        var reader = prepareAdjacencyDecompressingReader(targets);
        var consumed = new MutableIntValue() {};

        var remaining = targets.length;

        // advance beyond the first target
        var nextTarget = reader.advance(targets[0], remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[0]);
        assertThat(consumed.value).isEqualTo(1);
        remaining -= consumed.value;

        // advance to the end of the first block
        nextTarget = reader.advance(targets[63], remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[63]);
        assertThat(consumed.value).isEqualTo(63);
        remaining -= consumed.value;

        // advance beyond the first block
        nextTarget = reader.advance(targets[64], remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[64]);
        assertThat(consumed.value).isEqualTo(1);
        remaining -= consumed.value;

        // read to the end
        nextTarget = reader.advance(targets[127], remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[127]);
        assertThat(consumed.value).isEqualTo(63);
        remaining -= consumed.value;

        // read beyond the end
        nextTarget = reader.advance(targets[127] + 1, remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[127]);
        assertThat(consumed.value).isEqualTo(0);
    }

    @Test
    void testAdvanceBy() {
        var targets = LongStream.range(0, 128).toArray();
        var reader = prepareAdjacencyDecompressingReader(targets);
        var consumed = new MutableIntValue() {};

        var remaining = targets.length;

        // advance to the first element
        var nextTarget = reader.advanceBy(0, remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[0]);
        assertThat(consumed.value).isEqualTo(1);
        remaining -= consumed.value;

        // advance to the end of the first block
        nextTarget = reader.advanceBy(62, remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[63]);
        assertThat(consumed.value).isEqualTo(63);
        remaining -= consumed.value;

        // advance beyond the first block
        nextTarget = reader.advanceBy(0, remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[64]);
        assertThat(consumed.value).isEqualTo(1);
        remaining -= consumed.value;

        // read to the end
        nextTarget = reader.advanceBy(62, remaining, consumed);
        assertThat(nextTarget).isEqualTo(targets[127]);
        assertThat(consumed.value).isEqualTo(63);
        remaining -= consumed.value;

        // read beyond the end -- not possible since no more remaining
    }

    private AdjacencyDecompressingReader prepareAdjacencyDecompressingReader(long[] originalTargets) {
        var targets = originalTargets.clone();

        AdjacencyCompression.applyDeltaEncoding(targets, targets.length, Aggregation.NONE);
        var compressed = new byte[targets.length * Long.BYTES];
        int requiredBytes = AdjacencyCompression.compress(targets, compressed, targets.length);
        compressed = Arrays.copyOf(compressed, requiredBytes);

        var reader = new AdjacencyDecompressingReader();
        reader.reset(compressed, 0, targets.length);

        return reader;
    }
}
