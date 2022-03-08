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
package org.neo4j.gds.decisiontree;

import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

public class ClassificationDecisionTreeTrain<LOSS extends DecisionTreeLoss> extends DecisionTreeTrain<LOSS, Long> {

    private final HugeLongArray allLabels;
    private final LocalIdMap classIdMap;

    public ClassificationDecisionTreeTrain(
        LOSS lossFunction,
        HugeObjectArray<double[]> allFeatures,
        HugeLongArray allLabels,
        LocalIdMap classIdMap,
        DecisionTreeTrainConfig config,
        FeatureBagger featureBagger,
        double numFeatureVectorsRatio
        ) {
        super(
            allFeatures,
            config,
            lossFunction,
            featureBagger,
            numFeatureVectorsRatio
        );
        this.classIdMap = classIdMap;

        assert allLabels.size() == allFeatures.size();
        this.allLabels = allLabels;
    }

    @Override
    protected Long toTerminal(final HugeLongArray group, final long groupSize) {
        assert groupSize > 0;
        assert group.size() >= groupSize;

        final var classesInGroup = new long[classIdMap.size()];

        for (long i = 0; i < groupSize; i++) {
            long label = allLabels.get(group.get(i));
            classesInGroup[classIdMap.toMapped(label)]++;
        }

        long max = -1;
        int maxClassIdx = 0;
        for (int i = 0; i < classesInGroup.length; i++) {
            if (classesInGroup[i] <= max) continue;

            max = classesInGroup[i];
            maxClassIdx = i;
        }

        return classIdMap.toOriginal(maxClassIdx);
    }
}
