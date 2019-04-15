/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.similarity;

import org.HdrHistogram.DoubleHistogram;

import java.util.Comparator;
import java.util.Objects;

public class SimilarityResult implements Comparable<SimilarityResult> {
    public final long item1;
    public final long item2;
    public final long count1;
    public final long count2;
    public final long intersection;
    public double similarity;
    public final boolean bidirectional;
    public final boolean reversed;

    public static SimilarityResult TOMB = new SimilarityResult(-1, -1, -1, -1, -1, -1);

    public SimilarityResult(long item1, long item2, long count1, long count2, long intersection, double similarity, boolean bidirectional, boolean reversed) {
        this.item1 = item1;
        this.item2 = item2;
        this.count1 = count1;
        this.count2 = count2;
        this.intersection = intersection;
        this.similarity = similarity;
        this.bidirectional = bidirectional;
        this.reversed = reversed;
    }
    public SimilarityResult(long item1, long item2, long count1, long count2, long intersection, double similarity) {
        this(item1,item2, count1,count2,intersection,similarity, true, false);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimilarityResult that = (SimilarityResult) o;
        return item1 == that.item1 &&
                item2 == that.item2 &&
                count1 == that.count1 &&
                count2 == that.count2 &&
                intersection == that.intersection &&
                Double.compare(that.similarity, similarity) == 0 &&
                bidirectional == that.bidirectional &&
                reversed == that.reversed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(item1, item2, count1, count2, intersection, similarity, bidirectional, reversed);
    }

    /**
     * sorts by default descending
     */
    @Override
    public int compareTo(SimilarityResult o) {
        return Double.compare(o.similarity,this.similarity);
    }

    public SimilarityResult reverse() {
        return new SimilarityResult(item2, item1,count2,count1,intersection,similarity,bidirectional,!reversed);
    }

    public SimilarityResult squareRooted() {
        this.similarity = Math.sqrt(this.similarity);
        return  this;
    }

    void record(DoubleHistogram histogram) {
        try {
            histogram.recordValue(similarity);
        } catch (ArrayIndexOutOfBoundsException ignored) {
        }
    }
    static Comparator<SimilarityResult> ASCENDING = (o1, o2) -> -o1.compareTo(o2);
    static Comparator<SimilarityResult> DESCENDING = SimilarityResult::compareTo;

    @Override
    public String toString() {
        return "SimilarityResult{" +
                "item1=" + item1 +
                ", item2=" + item2 +
                ", count1=" + count1 +
                ", count2=" + count2 +
                ", intersection=" + intersection +
                ", similarity=" + similarity +
                ", bidirectional=" + bidirectional +
                ", reversed=" + reversed +
                '}';
    }
}
