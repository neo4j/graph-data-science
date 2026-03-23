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
package org.neo4j.gds.compression.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VarLongEncodingTest {

    @Test
    void encodedByteLength() {
        var targets1 = new long[]{1, 3, 3, 7};
        var targets2 = new long[]{1, 3, 3, 8, 9};

        int targets1EncodedSize = VarLongEncoding.encodedVLongsSize(targets1, 0, targets1.length);
        int targets2EncodedSize = VarLongEncoding.encodedVLongsSize(targets2, 0, targets2.length);

        byte[] page = new byte[1024];
        int into = 42;
        VarLongEncoding.encodeVLongs(targets1, 0, targets1.length, page, into);
        VarLongEncoding.encodeVLongs(targets2, 0, targets2.length, page, into + targets1EncodedSize);

        var result1 = VarLongEncoding.encodedByteLength(page, into, targets1.length);
        var result2 = VarLongEncoding.encodedByteLength(page, into + targets1EncodedSize, targets2.length);

        assertThat(result1).isEqualTo(targets1EncodedSize);
        assertThat(result2).isEqualTo(targets2EncodedSize);
    }

    @Test
    void encodedByteLengthFullWord() {
        var targets = new long[]{0, 1, 2, 3, 4, 5, 6, 7};

        int targetsEncodedSize = VarLongEncoding.encodedVLongsSize(targets, 0, targets.length);

        byte[] page = new byte[1024];
        int into = 0;
        VarLongEncoding.encodeVLongs(targets, 0, targets.length, page, into);

        int result = VarLongEncoding.encodedByteLength(page, into, targets.length);

        assertThat(result).isEqualTo(targetsEncodedSize);
    }

    @Test
    void encodedByteLengthOverflowOneWord() {
        var targets = new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8};

        int targetsEncodedSize = VarLongEncoding.encodedVLongsSize(targets, 0, targets.length);

        byte[] page = new byte[1024];
        int into = 0;
        VarLongEncoding.encodeVLongs(targets, 0, targets.length, page, into);

        int result = VarLongEncoding.encodedByteLength(page, into, targets.length);

        assertThat(result).isEqualTo(targetsEncodedSize);
    }

    @Test
    void encodedByteLengthTargetValueLargerThanOneWord() {
        // Maximum value that can be encoded in a single word is 56 bits long
        var targets = new long[]{1L << 57};

        int targetsEncodedSize = VarLongEncoding.encodedVLongsSize(targets, 0, targets.length);

        byte[] page = new byte[1024];
        int into = 0;
        VarLongEncoding.encodeVLongs(targets, 0, targets.length, page, into);

        int result = VarLongEncoding.encodedByteLength(page, into, targets.length);

        assertThat(result).isEqualTo(targetsEncodedSize);
    }

    @Test
    void encodedByteLengthWithLessThanOneWordLeftInPage() {
        var targets = new long[]{0, 1, 2, 3, 4, 5, 6};

        int targetsEncodedSize = VarLongEncoding.encodedVLongsSize(targets, 0, targets.length);

        byte[] page = new byte[56];
        int into = 0;
        VarLongEncoding.encodeVLongs(targets, 0, targets.length, page, into);

        int result = VarLongEncoding.encodedByteLength(page, into, targets.length);

        assertThat(result).isEqualTo(targetsEncodedSize);
    }

    @Test
    void encodedValueCount() {
        var targets1 = new long[]{1, 3, 3, 7};
        var targets2 = new long[]{1, 3, 3, 8, 9};

        int targets1EncodedSize = VarLongEncoding.encodedVLongsSize(targets1, 0, targets1.length);
        int targets2EncodedSize = VarLongEncoding.encodedVLongsSize(targets2, 0, targets2.length);

        byte[] page = new byte[1024];
        int into = 42;
        VarLongEncoding.encodeVLongs(targets1, 0, targets1.length, page, into);
        VarLongEncoding.encodeVLongs(targets2, 0, targets2.length, page, into + targets1EncodedSize);

        int result = VarLongEncoding.encodedValueCount(page, into, targets1EncodedSize);
        assertThat(result).isEqualTo(targets1.length);

        result = VarLongEncoding.encodedValueCount(page, into + targets1EncodedSize, targets2EncodedSize);
        assertThat(result).isEqualTo(targets2.length);
    }

    @Test
    void encodedValueCountFullWord() {
        var targets = new long[]{0, 1, 2, 3, 4, 5, 6, 7};

        int targetsEncodedSize = VarLongEncoding.encodedVLongsSize(targets, 0, targets.length);

        byte[] page = new byte[1024];
        int into = 0;
        VarLongEncoding.encodeVLongs(targets, 0, targets.length, page, into);

        int result = VarLongEncoding.encodedValueCount(page, into, targetsEncodedSize);

        assertThat(result).isEqualTo(targets.length);
    }

    @Test
    void encodedValueCountOverflowOneWord() {
        var targets = new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8};

        int targetsEncodedSize = VarLongEncoding.encodedVLongsSize(targets, 0, targets.length);

        byte[] page = new byte[1024];
        int into = 0;
        VarLongEncoding.encodeVLongs(targets, 0, targets.length, page, into);

        int result = VarLongEncoding.encodedValueCount(page, into, targetsEncodedSize);

        assertThat(result).isEqualTo(targets.length);
    }

    @Test
    void encodedValueCountValueLargerThanOneWord() {
        // Maximum value that can be encoded in a single word is 56 bits long
        var targets = new long[]{1L << 57};

        int targetsEncodedSize = VarLongEncoding.encodedVLongsSize(targets, 0, targets.length);

        byte[] page = new byte[1024];
        int into = 0;
        VarLongEncoding.encodeVLongs(targets, 0, targets.length, page, into);

        int result = VarLongEncoding.encodedValueCount(page, into, targetsEncodedSize);

        assertThat(result).isEqualTo(targets.length);
    }
}
