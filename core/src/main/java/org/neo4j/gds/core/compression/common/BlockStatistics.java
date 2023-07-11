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

import org.HdrHistogram.Histogram;
import org.neo4j.gds.mem.BitUtil;

public final class BlockStatistics implements AutoCloseable {

    public static final BlockStatistics EMPTY = new BlockStatistics();

    // used per block
    private final Histogram bitsPerValue;

    // used to collect across blocks
    private long blockCount;
    private final Histogram stdDevBits;
    private final Histogram meanBits;
    private final Histogram medianBits;
    private final Histogram blockLengths;
    private final Histogram maxBits;
    private final Histogram minBits;
    private final Histogram indexOfMaxValue;
    private final Histogram indexOfMinValue;
    private final Histogram headTailDiffBits;

    BlockStatistics() {
        this.bitsPerValue = new Histogram(0);

        this.blockCount = 0;
        this.stdDevBits = new Histogram(2);
        this.meanBits = new Histogram(0);
        this.medianBits = new Histogram(0);
        this.blockLengths = new Histogram(0);
        this.maxBits = new Histogram(0);
        this.minBits = new Histogram(0);
        this.indexOfMinValue = new Histogram(0);
        this.indexOfMaxValue = new Histogram(0);
        this.headTailDiffBits = new Histogram(0);
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
        this.blockLengths.recordValue(length);

        int headBits = bitsNeeded(values[start]);
        int tailBitsSum = 0;
        int indexOfMaxValue = start;
        int maxValue = 0;
        int indexOfMinValue = start;
        int minValue = Long.SIZE;

        for (int i = start; i < start + length; i++) {
            int bitsPerValue = bitsNeeded(values[i]);
            this.bitsPerValue.recordValue(bitsPerValue);

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
                this.headTailDiffBits.recordValue(headBits - tailBitsMean);
            }
        }
        this.stdDevBits.recordValue((long) Math.ceil(this.bitsPerValue.getStdDeviation()));
        this.meanBits.recordValue((long) Math.ceil(this.bitsPerValue.getMean()));
        this.medianBits.recordValue(this.bitsPerValue.getValueAtPercentile(50));
        this.maxBits.recordValue(maxValue);
        this.minBits.recordValue(minValue);
        this.indexOfMaxValue.recordValue(indexOfMaxValue - start);
        this.indexOfMinValue.recordValue(indexOfMinValue - start);
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
