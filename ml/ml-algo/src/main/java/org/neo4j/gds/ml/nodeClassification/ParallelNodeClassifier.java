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
package org.neo4j.gds.ml.nodeClassification;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.batch.BatchTransformer;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.Features;

public class ParallelNodeClassifier {

    private final Classifier classifier;
    private final Features features;
    private final int batchSize;
    private final Concurrency concurrency;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;

    ParallelNodeClassifier(
        Classifier classifier,
        Features features,
        int batchSize,
        Concurrency concurrency,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.classifier = classifier;
        this.features = features;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
    }

    public HugeIntArray predict(ReadOnlyHugeLongArray evaluationSet) {
        return predict(evaluationSet.size(), evaluationSet::get, null);
    }

    public HugeIntArray predict(@Nullable HugeObjectArray<double[]> predictedProbabilities) {
        return predict(features.size(), BatchTransformer.IDENTITY, predictedProbabilities);
    }

    private HugeIntArray predict(long evaluationSetSize, BatchTransformer batchTransformer, @Nullable HugeObjectArray<double[]> predictedProbabilities) {
        var predictedClasses = HugeIntArray.newArray(evaluationSetSize);
        var consumer = new NodeClassificationPredictConsumer(
            features,
            batchTransformer,
            classifier,
            predictedProbabilities,
            predictedClasses,
            progressTracker
        );
        BatchQueue.consecutive(evaluationSetSize, batchSize, concurrency)
            .parallelConsume(consumer, concurrency, terminationFlag);

        return predictedClasses;
    }
}
