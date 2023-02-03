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
package org.neo4j.gds.ml.models.randomforest;

import com.carrotsearch.hppc.ObjectArrayList;
import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredictor;
import org.neo4j.gds.ml.decisiontree.DecisionTreeTrainer;
import org.neo4j.gds.ml.decisiontree.TreeNode;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.TrainingMethod;

import java.util.List;
import java.util.function.LongUnaryOperator;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

@ValueClass
@SuppressWarnings("immutables:subtype")
public interface RandomForestClassifierData extends Classifier.ClassifierData {

    List<DecisionTreePredictor<Integer>> decisionTrees();

    @Value.Derived
    default TrainingMethod trainerMethod() {
        return TrainingMethod.RandomForestClassification;
    }

    static MemoryEstimation memoryEstimation(
        LongUnaryOperator numberOfTrainingExamples,
        RandomForestTrainerConfig config
    ) {
        return MemoryEstimations.builder("Random forest model data")
            .rangePerNode(
                "Decision trees",
                nodeCount ->
                    MemoryRange.of(sizeOfInstance(ObjectArrayList.class))
                        .add(DecisionTreeTrainer
                            .estimateTree(
                                config,
                                numberOfTrainingExamples.applyAsLong(nodeCount),
                                TreeNode.leafMemoryEstimation(Integer.class)
                            )
                            .times(config.numberOfDecisionTrees())
                        )
            )
            .build();
    }
}
