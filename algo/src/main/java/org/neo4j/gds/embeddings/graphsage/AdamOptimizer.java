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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Vector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;

// Division, squaring and square-rooting is done elementwise.
public class AdamOptimizer {

    private static final double CLIP_MAX = 5.0;
    private static final double CLIP_MIN = -5.0;
    private static final double DEFAULT_ALPHA = 0.001;

    // TODO: Pass these via config???
    private final double alpha;
    private final double beta_1 = 0.9;
    private final double beta_2 = 0.999;
    private final double epsilon = 1e-8;

    private final List<Weights<? extends Tensor<?>>> variables;

    private List<? extends Tensor<?>> momentumTerms;
    private List<? extends Tensor<?>> velocityTerms;

    private int iteration = 0;

    public static long sizeInBytes(int rows, int cols, int numberOfWeights) {
        var termSize = Weights.sizeInBytes(rows, cols) * numberOfWeights;
        return sizeOfInstance(AdamOptimizer.class) +
                2 * termSize + // fields
                4 * termSize; // working memory
    }

    public AdamOptimizer(List<Weights<? extends Tensor<?>>> variables) {
        this(variables, DEFAULT_ALPHA);
    }

    public AdamOptimizer(
        List<Weights<? extends Tensor<?>>> variables,
        double learningRate
    ) {
        alpha = learningRate;
        this.variables = variables;

        momentumTerms = variables.stream().map(v -> v.data().zeros()).collect(Collectors.toList());
        velocityTerms = List.copyOf(momentumTerms);
    }

    // TODO: probably doesnt have to be synchronized
    public synchronized void update(ComputationContext otherCtx) {
        iteration += 1;
        variables.forEach(variable -> otherCtx.gradient(variable).mapInPlace(this::clip));

        // m_t = beta_1*m_t + (1-beta_1)*g_t	#updates the moving averages of the gradient
        momentumTerms = IntStream.range(0, variables.size())
            .mapToObj(i -> {
                Variable<?> variable = variables.get(i);
                Tensor<?> momentumTerm = momentumTerms.get(i);
                return castAndAdd(
                    momentumTerm.scalarMultiply(beta_1),
                    otherCtx.gradient(variable).scalarMultiply(1 - beta_1)
                );
            })
            .collect(Collectors.toList());

        // v_t = beta_2*v_t + (1-beta_2)*(g_t*g_t)	#updates the moving averages of the squared gradient
        velocityTerms = IntStream.range(0, variables.size())
            .mapToObj(i -> {
                Variable<?> variable = variables.get(i);
                Tensor<?> velocityTerm = velocityTerms.get(i);
                Tensor<?> gradient = otherCtx.gradient(variable);
                Tensor<?> squaredGradient = gradient.elementwiseProduct(gradient);
                return castAndAdd(
                    velocityTerm.scalarMultiply(beta_2),
                    squaredGradient.scalarMultiply(1 - beta_2)
                );
            })
            .collect(Collectors.toList());

        // m_cap = m_t/(1-(beta_1**t))		#calculates the bias-corrected estimates
        List<Tensor<?>> mCaps = momentumTerms.stream()
            .map(mTerm -> mTerm.scalarMultiply(1d / (1 - Math.pow(beta_1, iteration))))
            .collect(Collectors.toList());

        // v_cap = v_t/(1-(beta_2**t))		#calculates the bias-corrected estimates
        List<Tensor<?>> vCaps = velocityTerms.stream()
            .map(vTerm -> vTerm.scalarMultiply(1d / (1 - Math.pow(beta_2, iteration))))
            .collect(Collectors.toList());

        IntStream.range(0, variables.size())
            .forEach(i -> {
                Weights<? extends Tensor<?>> variable = variables.get(i);
                Tensor<?> mCap = mCaps.get(i);
                Tensor<?> vCap = vCaps.get(i);

                // theta_0 = theta_0 - (alpha*m_cap)/(math.sqrt(v_cap)+epsilon)	#updates the parameters
                Tensor<?> theta_0 = variable.data();
                theta_0.addInPlace(mCap
                    .scalarMultiply(-alpha)
                    .elementwiseProduct(vCap.map(v -> 1 / (Math.sqrt(v) + epsilon)))
                );
            });
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
