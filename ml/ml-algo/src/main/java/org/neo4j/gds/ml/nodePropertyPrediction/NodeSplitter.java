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
package org.neo4j.gds.ml.nodePropertyPrediction;

import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.util.ShuffleUtil;

import java.util.List;
import java.util.Optional;
import java.util.SortedSet;

import static org.neo4j.gds.ml.util.ShuffleUtil.createRandomDataGenerator;
import static org.neo4j.gds.ml.util.TrainingSetWarnings.warnForSmallNodeSets;

public class NodeSplitter {

    private final long numberOfExamples;
    private final LongToLongFunction targets;
    private final SortedSet<Long> distinctTargets;
    private final ProgressTracker progressTracker;

    public NodeSplitter(
        long numberOfExamples,
        LongToLongFunction targets,
        SortedSet<Long> distinctTargets,
        ProgressTracker progressTracker
    ) {
        this.numberOfExamples = numberOfExamples;
        this.targets = targets;
        this.distinctTargets = distinctTargets;
        this.progressTracker = progressTracker;
    }

    public NodeSplits split(double testFraction, int validationFolds, Optional<Long> randomSeed) {
        var allTrainingExamples = HugeLongArray.newArray(numberOfExamples);
        allTrainingExamples.setAll(i -> i);

        ShuffleUtil.shuffleHugeLongArray(allTrainingExamples, createRandomDataGenerator(randomSeed));
        var outerSplit = new FractionSplitter().split(allTrainingExamples, 1 - testFraction);

        warnForSmallNodeSets(
            outerSplit.trainSet().size(),
            outerSplit.testSet().size(),
            validationFolds,
            progressTracker
        );

        List<TrainingExamplesSplit> innerSplits = new StratifiedKFoldSplitter(
            validationFolds,
            ReadOnlyHugeLongArray.of(outerSplit.trainSet()),
            targets,
            randomSeed,
            distinctTargets
        ).splits();


        return ImmutableNodeSplits.of(allTrainingExamples, outerSplit, innerSplits);
    }

    @ValueClass
    public interface NodeSplits {
        HugeLongArray allTrainingExamples();
        TrainingExamplesSplit outerSplit();
        List<TrainingExamplesSplit> innerSplits();
    }
}
