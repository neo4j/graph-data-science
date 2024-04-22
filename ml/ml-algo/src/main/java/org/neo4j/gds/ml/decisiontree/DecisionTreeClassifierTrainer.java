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

import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.ml.models.Features;

import static org.neo4j.gds.mem.Estimate.sizeOfInstance;
import static org.neo4j.gds.mem.Estimate.sizeOfLongArray;

public class DecisionTreeClassifierTrainer extends DecisionTreeTrainer<Integer> {

    private final HugeIntArray allLabels;
    private final int numberOfClasses;

    public DecisionTreeClassifierTrainer(
        ImpurityCriterion impurityCriterion,
        Features features,
        HugeIntArray labels,
        int numberOfClasses,
        DecisionTreeTrainerConfig config,
        FeatureBagger featureBagger
    ) {
        super(
            features,
            config,
            impurityCriterion,
            featureBagger
        );
        this.numberOfClasses = numberOfClasses;

        assert labels.size() == features.size();
        this.allLabels = labels;
    }

    public static MemoryRange memoryEstimation(
        DecisionTreeTrainerConfig config,
        long numberOfTrainingSamples,
        int numberOfClasses
    ) {
        return MemoryRange.of(sizeOfInstance(DecisionTreeClassifierTrainer.class))
            .add(DecisionTreeTrainer.estimateTree(
                config,
                numberOfTrainingSamples,
                TreeNode.leafMemoryEstimation(Integer.class),
                GiniIndex.GiniImpurityData.memoryEstimation(numberOfClasses)
            ))
            .add(sizeOfLongArray(numberOfClasses));
    }

    @Override
    protected Integer toTerminal(Group group) {
        final var classesInGroup = new long[numberOfClasses];
        var array = group.array();

        for (long i = group.startIdx(); i < group.startIdx() + group.size(); i++) {
            classesInGroup[allLabels.get(array.get(i))]++;
        }

        long maxClassCountInGroup = -1;
        int maxMappedClass = 0;
        for (int i = 0; i < classesInGroup.length; i++) {
            if (classesInGroup[i] <= maxClassCountInGroup) continue;

            maxClassCountInGroup = classesInGroup[i];
            maxMappedClass = i;
        }

        return maxMappedClass;
    }
}
