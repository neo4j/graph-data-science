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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.models.Features;
import org.neo4j.gds.models.Classifier;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.BatchTransformer;

import java.util.function.Consumer;

/**
 * Consumes a BatchQueue containing long indices into a <code>nodeIds</code> LongArrayAccessor.
 * The consumer will apply node classification to the ids in <code>nodeIds</code>
 * and write them in the same order into <code>predictedClasses</code>.
 * If <code>predictedProbabilities</code> is non-null, the predicted probabilities
 * will also be written into it.
 */
public class NodeClassificationPredictConsumer implements Consumer<Batch> {
    private final Features features;
    private final BatchTransformer nodeIds;
    private final Classifier classifier;
    private final HugeObjectArray<double[]> predictedProbabilities;
    private final HugeLongArray predictedClasses;
    private final ProgressTracker progressTracker;

    NodeClassificationPredictConsumer(
        Features features,
        BatchTransformer nodeIds,
        Classifier classifier,
        HugeObjectArray<double[]> predictedProbabilities,
        HugeLongArray predictedClasses,
        ProgressTracker progressTracker
    ) {
        this.features = features;
        this.nodeIds = nodeIds;
        this.classifier = classifier;
        this.predictedProbabilities = predictedProbabilities;
        this.predictedClasses = predictedClasses;
        this.progressTracker = progressTracker;
    }

    @Override
    public void accept(Batch batch) {
        var numberOfClasses = classifier.numberOfClasses();
        for (long nodeIndex : batch.nodeIds()) {
            var nodeId = nodeIds.apply(nodeIndex);
            var predictedProbabilitiesForNode = classifier.predictProbabilities(nodeId, features);
            if (predictedProbabilities != null) {
                predictedProbabilities.set(nodeIndex, predictedProbabilitiesForNode);
            }
            var bestClassId = -1;
            var maxProbability = -1d;

            // TODO: replace with a generic DoubleMatrixOperations.maxWithIndex (lookup correct name)
            for (int classId = 0; classId < numberOfClasses; classId++) {
                var probability = predictedProbabilitiesForNode[classId];
                if (probability > maxProbability) {
                    maxProbability = probability;
                    bestClassId = classId;
                }
            }

            long bestClass = classifier.classIdMap().toOriginal(bestClassId);
            predictedClasses.set(nodeIndex, bestClass);
        }
        progressTracker.logProgress(batch.size());
    }

}
