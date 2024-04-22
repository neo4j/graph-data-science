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

import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.ml.models.Features;

import static org.neo4j.gds.mem.Estimate.sizeOfInstance;

public class DecisionTreeRegressorTrainer extends DecisionTreeTrainer<Double> {

    private final HugeDoubleArray targets;

    public DecisionTreeRegressorTrainer(
        ImpurityCriterion impurityCriterion,
        Features features,
        HugeDoubleArray targets,
        DecisionTreeTrainerConfig config,
        FeatureBagger featureBagger
    ) {
        super(
            features,
            config,
            impurityCriterion,
            featureBagger
        );

        assert targets.size() == features.size();
        this.targets = targets;
    }

    public static MemoryRange memoryEstimation(
        DecisionTreeTrainerConfig config,
        long numberOfTrainingSamples
    ) {
        return MemoryRange.of(sizeOfInstance(DecisionTreeRegressorTrainer.class))
            .add(DecisionTreeTrainer.estimateTree(
                config,
                numberOfTrainingSamples,
                TreeNode.leafMemoryEstimation(Double.class),
                SplitMeanSquaredError.MSEImpurityData.memoryEstimation()
            ));
    }

    @Override
    protected Double toTerminal(Group group) {
        var array = group.array();

        double sum = 0;
        for (long i = group.startIdx(); i < group.startIdx() + group.size(); i++) {
            sum += targets.get(array.get(i));
        }

        return sum / group.size();
    }
}
