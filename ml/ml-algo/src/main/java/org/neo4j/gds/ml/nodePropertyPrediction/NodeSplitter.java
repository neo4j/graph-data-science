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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeMergeSort;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.util.ShuffleUtil;

import java.util.Optional;
import java.util.function.LongUnaryOperator;

import static org.neo4j.gds.ml.util.ShuffleUtil.createRandomDataGenerator;
import static org.neo4j.gds.ml.util.TrainingSetWarnings.warnForSmallNodeSets;

public final class NodeSplitter {

    private final int concurrency;
    private final long numberOfExamples;
    private final ProgressTracker progressTracker;
    private final LongUnaryOperator toOriginalId;
    private final LongUnaryOperator toMappedId;

    public NodeSplitter(
        int concurrency,
        long numberOfExamples,
        ProgressTracker progressTracker,
        LongUnaryOperator toOriginalId,
        LongUnaryOperator toMappedId
    ) {
        this.concurrency = concurrency;
        this.numberOfExamples = numberOfExamples;
        this.progressTracker = progressTracker;
        this.toOriginalId = toOriginalId;
        this.toMappedId = toMappedId;
    }

    public NodeSplits split(double testFraction, int validationFolds, Optional<Long> randomSeed) {
        var allTrainingExamples = HugeLongArray.newArray(numberOfExamples);
        // sorting the internal id's by the corresponding originalIds makes this deterministic
        // based on the original node-id space -- supporting different graph-projections
        allTrainingExamples.setAll(toOriginalId);
        HugeMergeSort.sort(allTrainingExamples, concurrency);
        allTrainingExamples.setAll(i -> toMappedId.applyAsLong(allTrainingExamples.get(i)));

        ShuffleUtil.shuffleArray(allTrainingExamples, createRandomDataGenerator(randomSeed));
        var outerSplit = new FractionSplitter().split(ReadOnlyHugeLongArray.of(allTrainingExamples), 1 - testFraction);

        warnForSmallNodeSets(
            outerSplit.trainSet().size(),
            outerSplit.testSet().size(),
            validationFolds,
            progressTracker
        );

        return ImmutableNodeSplits.of(ReadOnlyHugeLongArray.of(allTrainingExamples), outerSplit);
    }

    @ValueClass
    public interface NodeSplits {
        ReadOnlyHugeLongArray allTrainingExamples();
        TrainingExamplesSplit outerSplit();
    }
}
