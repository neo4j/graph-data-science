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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.core.utils.Intersections;

public class CosineSimilarityComputer implements MetricSimilarityComputer {
    private final double similarityCutoff;

    public CosineSimilarityComputer(double similarityCutoff) {
        this.similarityCutoff = similarityCutoff;
    }

    @Override
    public double computeSimilarity(long[] vector1, long[] vector2) {
        var intersection = Intersections.intersection3(vector1, vector2);
        var similarity = intersection / (Math.sqrt(vector1.length) * Math.sqrt(vector2.length));
        return similarity >= similarityCutoff ? similarity : Double.NaN;
    }

    @Override
    public double computeWeightedSimilarity(long[] vector1, long[] vector2, double[] weights1, double[] weights2) {
        assert vector1.length == weights1.length;
        assert vector2.length == weights2.length;

        double vector1SquaredSum = 0;
        double vector2SquaredSum = 0;
        double  above=0;

        int offset1 = 0;
        int offset2 = 0;
        int length1 = weights1.length;
        int length2 = weights2.length;

        while (offset1 < length1 && offset2 < length2) {
            long target1 = vector1[offset1];
            long target2 = vector2[offset2];
            double w1 = weights1[offset1];
            double w2 = weights2[offset2];

            if (target1 == target2) {
                above+=w1*w2;
                vector1SquaredSum+= w1*w1;
                vector2SquaredSum+= w2*w2;
                offset1++;
                offset2++;
            } else if (target1 < target2) {
                vector1SquaredSum += w1*w1;
                offset1++;
            } else {
                vector2SquaredSum += w2*w2;
                offset2++;
            }
        }

        for (; offset1 < length1; offset1++) {
            vector1SquaredSum += weights1[offset1] * weights1[offset1];
        }
        for (; offset2 < length2; offset2++) {
            vector2SquaredSum += weights2[offset2]* weights2[offset2];
        }


        double similarity = above/(Math.sqrt(vector1SquaredSum) * Math.sqrt(vector2SquaredSum));
        return similarity >= similarityCutoff ? similarity : Double.NaN;

    }

    static class Builder implements MetricSimilarityComputerBuilder {
        public MetricSimilarityComputer build(double similarityCutoff) {
            return new CosineSimilarityComputer(similarityCutoff);
        }

        @Override
        public String render() {
            return "COSINE";
        }
    }
}
