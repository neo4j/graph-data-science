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
package org.neo4j.gds.similarity.knn.metrics;

public final class Pearson {
    private Pearson() {}

    public static double floatMetric(float[] left, float[] right) {
        int len = Math.min(left.length, right.length);

        double leftSum = 0.0;
        double rightSum = 0.0;
        for (int i = 0; i < len; i++) {
            leftSum += left[i];
            rightSum += right[i];
        }

        double leftMean = leftSum / len;
        double rightMean = rightSum / len;

        double dotProductMinusMean = 0D;
        double xLength = 0D;
        double yLength = 0D;
        for (int i = 0; i < len; i++) {
            double leftDelta = left[i] - leftMean;
            double rightDelta = right[i] - rightMean;

            dotProductMinusMean += leftDelta * rightDelta;
            xLength += leftDelta * leftDelta;
            yLength += rightDelta * rightDelta;
        }

        double result = dotProductMinusMean / Math.sqrt(xLength * yLength);
        return Math.max(result, 0);
    }

    public static double doubleMetric(double[] left, double[] right) {
        int len = Math.min(left.length, right.length);

        double leftSum = 0.0;
        double rightSum = 0.0;
        for (int i = 0; i < len; i++) {
            leftSum += left[i];
            rightSum += right[i];
        }

        double leftMean = leftSum / len;
        double rightMean = rightSum / len;

        double dotProductMinusMean = 0D;
        double xLength = 0D;
        double yLength = 0D;
        for (int i = 0; i < len; i++) {
            double leftDelta = left[i] - leftMean;
            double rightDelta = right[i] - rightMean;

            dotProductMinusMean += leftDelta * rightDelta;
            xLength += leftDelta * leftDelta;
            yLength += rightDelta * rightDelta;
        }

        double result = dotProductMinusMean / Math.sqrt(xLength * yLength);
        return Math.max(result, 0);
    }
}
