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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.Predictor;
import org.neo4j.gds.ml.batch.Batch;
import org.neo4j.gds.ml.batch.BatchTransformer;
import org.neo4j.gds.ml.batch.MappedBatch;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

/**
 * Consumes a BatchQueue containing long indices into a <code>nodeIds</code> LongArrayAccessor.
 * The consumer will apply node classification to the ids in <code>nodeIds</code>
 * and write them in the same order into <code>predictedClasses</code>.
 * If <code>predictedProbabilities</code> is non-null, the predicted probabilities
 * will also be written into it.
 */
public class NodeClassificationPredictConsumer implements Consumer<Batch> {
    private final Graph graph;
    private final BatchTransformer nodeIds;
    private final Predictor<Matrix, MultiClassNLRData> predictor;
    private final HugeObjectArray<double[]> predictedProbabilities;
    private final HugeLongArray predictedClasses;
    private final List<String> featureProperties;
    private final ProgressLogger progressLogger;

    public NodeClassificationPredictConsumer(
        Graph graph,
        BatchTransformer nodeIds,
        Predictor<Matrix, MultiClassNLRData> predictor,
        @Nullable HugeObjectArray<double[]> predictedProbabilities,
        HugeLongArray predictedClasses,
        List<String> featureProperties,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.nodeIds = nodeIds;
        this.predictor = predictor;
        this.predictedProbabilities = predictedProbabilities;
        this.predictedClasses = predictedClasses;
        this.featureProperties = featureProperties;
        this.progressLogger = progressLogger;
    }

    @Override
    public void accept(Batch batch) {
        var originalNodeIdsBatch = new MappedBatch(batch, nodeIds);
        var probabilityMatrix = predictor.predict(graph, originalNodeIdsBatch);
        var numberOfClasses = probabilityMatrix.cols();
        var probabilities = probabilityMatrix.data();
        var currentRow = 0;
        for (long nodeIndex : batch.nodeIds()) {
            var offset = currentRow * numberOfClasses;
            if (predictedProbabilities != null) {
                var probabilitiesForNode = new double[numberOfClasses];
                System.arraycopy(probabilities, offset, probabilitiesForNode, 0, numberOfClasses);
                predictedProbabilities.set(nodeIndex, probabilitiesForNode);
            }
            var bestClassId = -1;
            var maxProbability = -1d;
            for (int classId = 0; classId < numberOfClasses; classId++) {
                var probability = probabilities[offset + classId];
                if (probability > maxProbability) {
                    maxProbability = probability;
                    bestClassId = classId;
                }
            }
            if (bestClassId == -1) {
                // TODO: Fail training if weights are NaN
                fail(nodeIds.apply(nodeIndex));
            }
            var bestClass = predictor.modelData().classIdMap().toOriginal(bestClassId);
            predictedClasses.set(nodeIndex, bestClass);
            currentRow++;
        }
        progressLogger.logProgress(batch.size());
    }

    private void fail(long nodeId) {
        var badProperties = new ArrayList<String>();
        for (var prop : featureProperties) {
            var theProp = graph.nodeProperties(prop);
            var valueType = theProp.valueType();
            if (valueType == ValueType.DOUBLE) {
                if (Double.isNaN(theProp.doubleValue(nodeId))) {
                    badProperties.add(prop);
                }
            } else if (valueType == ValueType.DOUBLE_ARRAY || valueType == ValueType.FLOAT_ARRAY) {
                for (double val : theProp.doubleArrayValue(nodeId)) {
                    if (Double.isNaN(val)) {
                        badProperties.add(prop);
                        break;
                    }
                }
            }

        }
        throw new IllegalArgumentException(
            formatWithLocale("Node with ID %d has invalid feature property value NaN. Properties with NaN values: %s",
                graph.toOriginalNodeId(nodeId),
                badProperties
            )
        );
    }
}
