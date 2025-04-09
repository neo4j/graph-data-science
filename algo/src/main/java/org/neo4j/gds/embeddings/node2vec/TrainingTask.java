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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.tensor.FloatVector;

import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.addInPlace;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.scale;

final class TrainingTask implements Runnable {
    private final HugeObjectArray<FloatVector> centerEmbeddings;
    private final HugeObjectArray<FloatVector> contextEmbeddings;

    private final PositiveSampleProducer positiveSampleProducer;
    private final NegativeSampleProducer negativeSampleProducer;
    private final FloatVector centerGradientBuffer;
    private final FloatVector contextGradientBuffer;
    private final int negativeSamplingRate;
    private final float learningRate;

    private final ProgressTracker progressTracker;

    private double lossSum;

    TrainingTask(
        HugeObjectArray<FloatVector> centerEmbeddings,
        HugeObjectArray<FloatVector> contextEmbeddings,
        PositiveSampleProducer positiveSampleProducer,
        NegativeSampleProducer negativeSampleProducer,
        float learningRate,
        int negativeSamplingRate,
        int embeddingDimensions,
        ProgressTracker progressTracker
    ) {
        this.centerEmbeddings = centerEmbeddings;
        this.contextEmbeddings = contextEmbeddings;
        this.positiveSampleProducer = positiveSampleProducer;
        this.negativeSampleProducer = negativeSampleProducer;
        this.learningRate = learningRate;
        this.negativeSamplingRate = negativeSamplingRate;

        this.centerGradientBuffer = new FloatVector(embeddingDimensions);
        this.contextGradientBuffer = new FloatVector(embeddingDimensions);
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        var buffer = new long[2];

        // this corresponds to a stochastic optimizer as the embeddings are updated after each sample
        while (positiveSampleProducer.next(buffer)) {
            trainPositiveSample(buffer[0], buffer[1]);

            for (var i = 0; i < negativeSamplingRate; i++) {
                trainNegativeSample(buffer[0], negativeSampleProducer.next());
            }
            progressTracker.logProgress();
        }
    }

    void trainPositiveSample(long center, long context) {
        var centerEmbedding = centerEmbeddings.get(center);
        var contextEmbedding = contextEmbeddings.get(context);

        var scaledGradient = computePositiveGradient(centerEmbedding, contextEmbedding);

        updateEmbeddings(
            centerEmbedding,
            contextEmbedding,
            scaledGradient,
            centerGradientBuffer,
            contextGradientBuffer
        );
    }

    void trainNegativeSample(long center, long context) {
        var centerEmbedding = centerEmbeddings.get(center);
        var contextEmbedding = contextEmbeddings.get(context);

        var scaledGradient = computeNegativeGradient(centerEmbedding, contextEmbedding);

        updateEmbeddings(
            centerEmbedding,
            contextEmbedding,
            scaledGradient,
            centerGradientBuffer,
            contextGradientBuffer
        );
    }

    float computePositiveGradient(FloatVector centerEmbedding, FloatVector contextEmbedding) {
        // L_pos = -log sigmoid(center * context)  ; gradient: -sigmoid (-center * context)
        // L_neg = -log sigmoid(-center * context) ; gradient: sigmoid (center * context)
        float affinity = centerEmbedding.innerProduct(contextEmbedding);
        //When |affinity| > 40, positiveSigmoid = 1. Double precision is not enough.
        //Make sure negativeSigmoid can never be 0 to avoid infinity loss.
        double positiveSigmoid = Sigmoid.sigmoid(affinity);
        double negativeSigmoid = 1 - positiveSigmoid;

        lossSum -= Math.log(positiveSigmoid + Node2VecModel.EPSILON);

        float gradient = (float) -negativeSigmoid;
        // we are doing gradient descent, so we go in the negative direction of the gradient here
        return -gradient * learningRate;
    }

    float computeNegativeGradient(FloatVector centerEmbedding, FloatVector contextEmbedding) {
        // L_pos = -log sigmoid(center * context)  ; gradient: -sigmoid (-center * context)
        // L_neg = -log sigmoid(-center * context) ; gradient: sigmoid (center * context)
        float affinity = centerEmbedding.innerProduct(contextEmbedding);
        //When |affinity| > 40, positiveSigmoid = 1. Double precision is not enough.
        //Make sure negativeSigmoid can never be 0 to avoid infinity loss.
        double positiveSigmoid = Sigmoid.sigmoid(affinity);
        double negativeSigmoid = 1 - positiveSigmoid;

        lossSum -= Math.log(negativeSigmoid + Node2VecModel.EPSILON);

        float gradient = (float) positiveSigmoid;
        // we are doing gradient descent, so we go in the negative direction of the gradient here
        return -gradient * learningRate;
    }

    void updateEmbeddings(
        FloatVector centerEmbedding,
        FloatVector contextEmbedding,
        float scaledGradient,
        FloatVector centerGradientBuffer,
        FloatVector contextGradientBuffer
    ) {
        scale(contextEmbedding.data(), scaledGradient, centerGradientBuffer.data());
        scale(centerEmbedding.data(), scaledGradient, contextGradientBuffer.data());

        addInPlace(centerEmbedding.data(), centerGradientBuffer.data());
        addInPlace(contextEmbedding.data(), contextGradientBuffer.data());
    }

    double lossSum() {
        return lossSum;
    }

}
