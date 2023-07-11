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
package org.neo4j.gds.core.compression.common;

import org.neo4j.gds.core.compression.BoundedHistogram;
import org.neo4j.gds.core.compression.packed.AdjacencyPacking;
import org.neo4j.gds.mem.BitUtil;

public final class BlockStatistics implements AutoCloseable {

    public static final BlockStatistics EMPTY = new BlockStatistics();

    // used per block
    private final BoundedHistogram bitsPerValue;

    // used to collect across blocks
    private long blockCount;
    private final BoundedHistogram stdDevBits;
    private final BoundedHistogram meanBits;
    private final BoundedHistogram medianBits;
    private final BoundedHistogram blockLengths;
    private final BoundedHistogram maxBits;
    private final BoundedHistogram minBits;
    private final BoundedHistogram indexOfMaxValue;
    private final BoundedHistogram indexOfMinValue;
    private final BoundedHistogram headTailDiffBits;

    BlockStatistics() {
        this.blockCount = 0;
        this.bitsPerValue = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
        this.stdDevBits = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
        this.meanBits = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
        this.medianBits = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
        this.blockLengths = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
        this.maxBits = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
        this.minBits = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
        this.indexOfMinValue = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
        this.indexOfMaxValue = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
        this.headTailDiffBits = new BoundedHistogram(AdjacencyPacking.BLOCK_SIZE);
    }

    public long blockCount() {
        return blockCount;
    }

    public ImmutableHistogram stdDevBits() {
        return ImmutableHistogram.of(stdDevBits);
    }

    public ImmutableHistogram meanBits() {
        return ImmutableHistogram.of(meanBits);
    }

    public ImmutableHistogram medianBits() {
        return ImmutableHistogram.of(medianBits);
    }

    public ImmutableHistogram blockLengths() {
        return ImmutableHistogram.of(blockLengths);
    }

    public ImmutableHistogram maxBits() {
        return ImmutableHistogram.of(maxBits);
    }

    public ImmutableHistogram minBits() {
        return ImmutableHistogram.of(minBits);
    }

    public ImmutableHistogram indexOfMinValue() {
        return ImmutableHistogram.of(indexOfMinValue);
    }

    public ImmutableHistogram indexOfMaxValue() {
        return ImmutableHistogram.of(indexOfMaxValue);
    }

    public ImmutableHistogram headTailDiffBits() {
        return ImmutableHistogram.of(headTailDiffBits);
    }

    public void record(long[] values, int start, int length) {
        this.bitsPerValue.reset();

        this.blockCount++;
        this.blockLengths.record(length);

        int headBits = bitsNeeded(values[start]);
        int tailBitsSum = 0;
        int indexOfMaxValue = start;
        int maxValue = 0;
        int indexOfMinValue = start;
        int minValue = Long.SIZE;

        for (int i = start; i < start + length; i++) {
            int bitsPerValue = bitsNeeded(values[i]);
            this.bitsPerValue.record(bitsPerValue);

            if (bitsPerValue > maxValue) {
                indexOfMaxValue = i;
                maxValue = bitsPerValue;
            }
            if (bitsPerValue < minValue) {
                indexOfMinValue = i;
                minValue = bitsPerValue;
            }

            if (i - start > 0) {
                tailBitsSum += bitsPerValue;
            }
        }

        if (tailBitsSum > 0) {
            int tailBitsMean = BitUtil.ceilDiv(tailBitsSum, length - 1);
            if (tailBitsMean <= headBits) {
                this.headTailDiffBits.record(headBits - tailBitsMean);
            }
        }
        this.stdDevBits.record((int) Math.ceil(this.bitsPerValue.stdDev()));
        this.meanBits.record((int) Math.ceil(this.bitsPerValue.mean()));
        this.medianBits.record(this.bitsPerValue.median());
        this.maxBits.record(maxValue);
        this.minBits.record(minValue);
        this.indexOfMaxValue.record(indexOfMaxValue - start);
        this.indexOfMinValue.record(indexOfMinValue - start);
    }

    void mergeInto(BlockStatistics other) {
        other.blockCount += this.blockCount;
        other.minBits.add(this.minBits);
        other.maxBits.add(this.maxBits);
        other.medianBits.add(this.medianBits);
        other.meanBits.add(this.meanBits);
        other.stdDevBits.add(this.stdDevBits);
        other.blockLengths.add(this.blockLengths);
        other.indexOfMaxValue.add(this.indexOfMaxValue);
        other.indexOfMinValue.add(this.indexOfMinValue);
        other.headTailDiffBits.add(this.headTailDiffBits);
    }

    @Override
    public void close() throws Exception {

    }

    private static int bitsNeeded(long value) {
        return Long.SIZE - Long.numberOfLeadingZeros(value);
    }
}
