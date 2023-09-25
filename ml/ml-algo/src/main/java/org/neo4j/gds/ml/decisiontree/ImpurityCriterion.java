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
package org.neo4j.gds.ml.decisiontree;

import org.neo4j.gds.collections.ha.HugeLongArray;

public interface ImpurityCriterion {
    ImpurityData groupImpurity(HugeLongArray group, long startIdx, long size);

    void incrementalImpurity(long featureVectorIdx, ImpurityData impurityData);

    void decrementalImpurity(long featureVectorIdx, ImpurityData impurityData);

    default double combinedImpurity(ImpurityData leftImpurityData, ImpurityData rightImpurityData) {
        long totalSize = leftImpurityData.groupSize() + rightImpurityData.groupSize();
        double leftWeight = (double) leftImpurityData.groupSize() / totalSize;
        double rightWeight = (double) rightImpurityData.groupSize() / totalSize;
        return leftWeight * leftImpurityData.impurity() + rightWeight * rightImpurityData.impurity();
    }

    /**
     * A lightweight representation of a decision tree node's impurity.
     */
    interface ImpurityData {
        double impurity();

        long groupSize();

        /**
         * Copies all significant data to `impurityData`.
         *
         * Can be called frequently during split searching so should be as lightweight as possible.
         *
         * @param impurityData  the object to copy to
         */
        void copyTo(ImpurityData impurityData);
    }
}
