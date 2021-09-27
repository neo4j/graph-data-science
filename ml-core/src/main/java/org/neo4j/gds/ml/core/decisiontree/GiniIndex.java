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
package org.neo4j.gds.ml.core.decisiontree;

public class GiniIndex implements DecisionTreeLoss {

    private final int[] classes;
    private final int[] allLabels;

    public GiniIndex(int[] classes, int[] allLabels) {
        this.classes = classes;
        this.allLabels = allLabels;
    }

    @Override
    public double splitLoss(int[][] groups, int[] groupSizes) {
        assert groups.length > 1;
        assert groups.length == groupSizes.length;

        double loss = 0;
        long totalSize = 0;

        for (int i = 0; i < groupSizes.length; i++) {
            loss += computeGroupLoss(groups[i], groupSizes[i]);
            totalSize += groupSizes[i];
        }

        if (totalSize == 0) {
            throw new IllegalStateException("Cannot compute loss over only empty groups");
        }

        return loss / totalSize;
    }

    private double computeGroupLoss(int[] group, int groupSize) {
        assert group.length >= groupSize;

        if (groupSize == 0) return 0;

        var groupClassCounts = new int[classes.length];
        for (int i = 0; i < groupSize; i++) {
            groupClassCounts[allLabels[group[i]]]++;
        }

        double score = 0.0;
        for (var count : groupClassCounts) {
            score += Math.pow(count, 2);
        }

        score /= groupSize;

        return groupSize - score;
    }
}
