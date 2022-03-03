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
package org.neo4j.gds.ml.core.optimizer;

import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Tensor;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

// Division, squaring and square-rooting is done element-wise.
// Based on https://arxiv.org/pdf/1412.6980.pdf
public class AdamOptimizer implements Updater {

    private static final double CLIP_MAX = 5.0;
    private static final double CLIP_MIN = -5.0;
    private static final double DEFAULT_ALPHA = 0.001;

    // TODO: Pass these via config???
    private final double alpha;
    private final double beta_1 = 0.9;
    private final double beta_2 = 0.999;
    private final double epsilon = 1e-8;

    private final List<Weights<? extends Tensor<?>>> weights;

    final List<Tensor<?>> momentumTerms;
    final List<Tensor<?>> velocityTerms;

    private int iteration = 0;

    public static long sizeInBytes(int rows, int cols, int numberOfWeights) {
        var termSize = Weights.sizeInBytes(rows, cols) * numberOfWeights;
        return sizeOfInstance(AdamOptimizer.class) +
                2 * termSize + // fields
                2 * termSize; // working memory: mCap, vCap
    }

    public AdamOptimizer(List<Weights<? extends Tensor<?>>> weights) {
        this(weights, DEFAULT_ALPHA);
    }

    public AdamOptimizer(
        List<Weights<? extends Tensor<?>>> weights,
        double learningRate
    ) {
        this.alpha = learningRate;
        this.weights = weights;
        this.momentumTerms = weights.stream().map(v -> v.data().createWithSameDimensions()).collect(Collectors.toList());
        this.velocityTerms = weights.stream().map(v -> v.data().createWithSameDimensions()).collect(Collectors.toList());
    }

    public void update(List<? extends Tensor<?>> contextLocalWeightGradients) {
        iteration += 1;

        for (int i = 0; i < weights.size(); i++) {
            var weight = this.weights.get(i).data();
            var gradient = contextLocalWeightGradients.get(i);
            var momentumTerm = momentumTerms.get(i);
            var velocityTerm = velocityTerms.get(i);

            // clip gradient to avoid exploding gradients
            gradient.mapInPlace(this::clip);

            // In-Place update momentum term
            // m_t = beta_1 * m_t + (1 - beta_1) * g_t
            momentumTerm.scalarMultiplyMutate(beta_1).addInPlace(gradient.scalarMultiply(1 - beta_1));

            // In-Place updates the velocity terms
            // v_t = beta_2 * v_t + (1 - beta_2) * (g_t^2)
            // ! reusing the memory of `gradient` for the `squaredGradient`
            var squaredGradient = gradient.mapInPlace(v -> v * v);
            velocityTerm.scalarMultiplyMutate(beta_2).addInPlace(squaredGradient.scalarMultiplyMutate(1 - beta_2));

            // m_cap = m_t / (1 - beta_1^t)		#calculates the bias-corrected estimates
            var mCap = momentumTerm.scalarMultiply(1d / (1 - Math.pow(beta_1, iteration)));

            // v_cap = v_t / (1 - beta_2^t)		#calculates the bias-corrected estimates
            var vCap = velocityTerm.scalarMultiply(1d / (1 - Math.pow(beta_2, iteration)));

            // theta_0 = theta_0 - (alpha * m_cap) / (math.sqrt(v_cap) + epsilon)	#updates the parameters
            weight.addInPlace(mCap
                .scalarMultiplyMutate(-alpha)
                .elementwiseProductMutate(vCap.mapInPlace(v -> 1 / (Math.sqrt(v) + epsilon)))
            );
        }
    }

    private double clip(double value) {
        if (value > CLIP_MAX) {
            return CLIP_MAX;
        }
        if (value < CLIP_MIN) {
            return CLIP_MIN;
        }
        return value;
    }
}
