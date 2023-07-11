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
package org.neo4j.gds.core.compression;

import java.util.Arrays;

/**
 * A simple, exact histogram implementation that is used for small domain spaces.
 * It's main purpose is tracking statistics for compression related logic.
 */
public class BoundedHistogram {

    public static final int NO_VALUE = -1;

    private int[] histogram;
    private int ub;
    private int total;

    /**
     * Create a new histogram that can record values between 0 and `upperBoundExclusive`.
     */
    public BoundedHistogram(int upperBoundExclusive) {
        this.histogram = new int[upperBoundExclusive];
        this.ub = upperBoundExclusive;
        this.total = 0;
    }

    /**
     * Record the occurrence of the value in the histogram.
     */
    public void record(int value) {
        if (value < ub) {
            this.histogram[value]++;
            this.total++;
        } else {
            throw new IllegalArgumentException("Value " + value + " is larger than the histogram upper bound " + ub);
        }
    }

    /**
     * Returns the total number of recorded values.
     */
    public int total() {
       return this.total;
    }

    /**
     * Return the average value recorded.
     */
    public int mean() {
        int sum = 0;

        int[] histogram = this.histogram;
        for (int i = 0; i < histogram.length; i++) {
            sum += histogram[i] * i;
        }

        return sum / this.total;
    }

    /**
     * Return the median value recorded.
     */
    public int median() {
        return percentile(50);
    }

    /**
     * Return the value that `percentile` percent of all values fall below.
     */
    public int percentile(float percentile) {
        int count = 0;
        int limit = Math.round(total * (percentile / 100));

        int[] histogram = this.histogram;
        for (int i = 0; i < histogram.length; i++) {
            count += histogram[i];
            if (count > limit) {
                return i;
            }
        }

        return histogram.length - 1;
    }

    /**
     * Return the standard deviation across all values.
     */
    public double stdDev() {
        int mean = mean();
        double sum = 0;

        int[] histogram = this.histogram;
        for (int i = 0; i < histogram.length; i++) {
            sum += Math.pow(i - mean, 2) * histogram[i];
        }

        return Math.sqrt(sum / this.total);
    }

    /**
     * Returns the lowest recorded value in the histogram.
     */
    public int min() {
        int[] histogram = this.histogram;
        for (int i = 0; i < histogram.length; i++) {
            if (histogram[i] > 0) {
                return i;
            }
        }
        return NO_VALUE;
    }

    /**
     * Returns the highest recorded value in the histogram.
     */
    public int max() {
        int[] histogram = this.histogram;
        for (int i = histogram.length - 1; i >= 0; i--) {
            if (histogram[i] > 0) {
                return i;
            }
        }
        return NO_VALUE;
    }

    /**
     * Reset the recorded values within the histogram.
     */
    public void reset() {
        Arrays.fill(this.histogram, 0);
        this.total = 0;
    }

    /**
     * Adds all recorded values of `other` to `this` histogram.
     * Potentially grows the value space of `this` histogram.
     */
    public void add(BoundedHistogram other) {
        if (other.ub > this.ub) {
            this.histogram = Arrays.copyOf(this.histogram, other.ub);
            this.ub = other.ub;
        }

        for (int otherValue = 0; otherValue < other.histogram.length; otherValue++) {
            for (int otherFrequency = 0; otherFrequency < other.histogram[otherValue]; otherFrequency++) {
                this.record(otherValue);
            }
        }
    }
}
