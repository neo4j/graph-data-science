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

import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;

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

    private final List<Tensor<?>> momentumTerms;
    private final List<Tensor<?>> velocityTerms;

    private int iteration = 0;

    public static long sizeInBytes(int rows, int cols, int numberOfWeights) {
        var termSize = Weights.sizeInBytes(rows, cols) * numberOfWeights;
        return sizeOfInstance(AdamOptimizer.class) +
                2 * termSize + // fields
                4 * termSize; // working memory
    }

    public AdamOptimizer(List<Weights<? extends Tensor<?>>> weights) {
        this(weights, DEFAULT_ALPHA);
    }

    public AdamOptimizer(
        List<Weights<? extends Tensor<?>>> weights,
        double learningRate
    ) {
        alpha = learningRate;
        this.weights = weights;

        momentumTerms = weights.stream().map(v -> v.data().zeros()).collect(Collectors.toList());
        velocityTerms = new ArrayList<>(momentumTerms);
    }

    // TODO: probably doesnt have to be synchronized
    public synchronized void update(ComputationContext otherCtx) {
        var localWeightGradients = weights.stream().map(otherCtx::gradient).collect(Collectors.toList());
        update(localWeightGradients);

    }

    public synchronized void update(List<? extends Tensor<?>> contextLocalWeightGradients) {
        iteration += 1;

        for (int i = 0; i < weights.size(); i++) {
            var weight = this.weights.get(i);
            var gradient = contextLocalWeightGradients.get(i).mapInPlace(this::clip);

            // m_t = beta_1 * m_t + (1 - beta_1) * g_t
            var momentumTerm = momentumTerms.get(i);
            var updatedMomentumTerm = castAndAdd(
                momentumTerm.scalarMultiply(beta_1),
                gradient.scalarMultiply(1 - beta_1)
            );

            // v_t = beta_2 * v_t + (1 - beta_2) * (g_t^2)
            var velocityTerm = velocityTerms.get(i);
            var squaredGradient = gradient.elementwiseProduct(gradient);
            var updatedVelocityTerm = castAndAdd(
                velocityTerm.scalarMultiply(beta_2),
                squaredGradient.scalarMultiply(1 - beta_2)
            );

            // m_cap = m_t / (1 - beta_1^t)		#calculates the bias-corrected estimates
            var mCap = updatedMomentumTerm.scalarMultiply(1d / (1 - Math.pow(beta_1, iteration)));

            // v_cap = v_t / (1 - beta_2^t)		#calculates the bias-corrected estimates
            var vCap = updatedVelocityTerm.scalarMultiply(1d / (1 - Math.pow(beta_2, iteration)));

            // theta_0 = theta_0 - (alpha * m_cap) / (math.sqrt(v_cap) + epsilon)	#updates the parameters
            var theta_0 = weight.data();
            theta_0.addInPlace(mCap
                .scalarMultiply(-alpha)
                .elementwiseProduct(vCap.map(v -> 1 / (Math.sqrt(v) + epsilon)))
            );

            // Updates the moving averages of the gradient
            momentumTerms.set(i, updatedMomentumTerm);
            // Updates the moving averages of the squared gradient
            velocityTerms.set(i, updatedVelocityTerm);
        }
    }

    // TODO: Try to retain type information and avoid these checks
    private Tensor<?> castAndAdd(Tensor<?> op1, Tensor<?> op2) {
        if (op1 instanceof Scalar && op2 instanceof Scalar) {
            return ((Scalar) op1).add(((Scalar) op2));
        } else if (op1 instanceof Vector && op2 instanceof Vector) {
            return ((Vector) op1).add(((Vector) op2));
        } else if (op1 instanceof Matrix && op2 instanceof Matrix) {
            return ((Matrix) op1).add(((Matrix) op2));
        } else {
            throw new IllegalStateException("Mismatched tensors! Can only add same types");
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
