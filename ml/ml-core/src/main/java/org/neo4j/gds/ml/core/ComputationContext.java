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
package org.neo4j.gds.ml.core;

import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ml.core.functions.SingleParentVariable;
import org.neo4j.gds.ml.core.tensor.Tensor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * The computation context is used for forward and backward propagation over a computation graphs consiting of {@link org.neo4j.gds.ml.core.Variable}s.
 * This implementation is not thread-safe!
 */
public class ComputationContext {
    private final Map<Variable<?>, Tensor<?>> data;
    private final Map<Variable<?>, Tensor<?>> gradients;

    public ComputationContext() {
        this.data = new HashMap<>();
        this.gradients = new HashMap<>();
    }

    // Only one forward call is expected for the caching strategy
    public <T extends Tensor<T>> T forward(Variable<T> variable) {
        var cachedData = (T) data.get(variable);
        if (cachedData != null) {
            return cachedData;
        }
        for (Variable<?> parent : variable.parents()) {
            forward(parent);
        }
        T variableResult = variable.apply(this);
        data.put(variable, variableResult);
        return variableResult;
    }

    public <T extends Tensor<T>> T data(Variable<T> variable) {
        return (T) data.get(variable);
    }

    public <T extends Tensor<T>> T gradient(Variable<T> variable) {
        return (T) gradients.get(variable);
    }

    public void backward(Variable<?> function) {
        assert (Dimensions.isScalar(function.dimensions())) : "Root variable must be scalar.";
        assert function.requireGradient() : "Root variable must have requireGradient==true";

        gradients.clear();
        Queue<BackPropTask> executionQueue = new LinkedBlockingQueue<>();
        var dummy = new PassThroughVariable<>(function);
        executionQueue.add(new BackPropTask(function, dummy));
        Map<Variable<?>, MutableInt> upstreamCounters = new HashMap<>();
        initUpstream(dummy, upstreamCounters);
        backward(executionQueue, upstreamCounters);
    }

    private void backward(Queue<BackPropTask> executionQueue, Map<Variable<?>, MutableInt> upstreamCounters) {
        while (!executionQueue.isEmpty()) {
            BackPropTask task = executionQueue.poll();
            var variable = task.variable;
            var child = task.child;
            Tensor<?> gradient = child.gradient(variable, this);
            updateGradient(variable, gradient);

            if (upstreamCounters.get(variable).decrementAndGet() == 0) {
                for (Variable<?> parent : variable.parents()) {
                    if (parent.requireGradient()) {
                        executionQueue.offer(new BackPropTask(parent, variable));
                    }
                }
            }
        }
    }

    private void initUpstream(Variable<?> function, Map<Variable<?>, MutableInt> upstreamCounters) {
        for (Variable<?> parent : function.parents()) {
            if (parent.requireGradient()) {
                boolean firstToSeeParent = !upstreamCounters.containsKey(parent);
                if (firstToSeeParent) {
                    initUpstream(parent, upstreamCounters);
                    upstreamCounters.put(parent, new MutableInt(0));
                }
                upstreamCounters.get(parent).increment();
            }
        }
    }

    private void updateGradient(Variable<?> variable, Tensor<?> gradient) {
        if (gradients.containsKey(variable)) {
            gradients.get(variable).addInPlace(gradient);
        } else {
            gradients.put(variable, gradient);
        }
    }

    public String render() {
        StringBuilder result = new StringBuilder();

        data.forEach((variable, dataEntry) -> {
            result.append(variable)
                .append(System.lineSeparator())
                .append("\t data: ")
                .append(dataEntry)
                .append(System.lineSeparator());

            var gradient = Optional.ofNullable(gradients.get(variable)).map(Tensor::toString);
            result.append("\t gradient: ").append(gradient.orElse("None")).append(System.lineSeparator());
        });

        renderOrphanGradients(result);

        return result.toString();

    }

    @TestOnly
    public Set<Variable<?>> computedVariables() {
        return data.keySet();
    }

    private void renderOrphanGradients(StringBuilder result) {
        var expectedVariables = data.keySet();
        var unmatchedGradients = gradients
            .entrySet()
            .stream()
            .filter(entry -> !expectedVariables.contains(entry.getKey()))
            .collect(Collectors.toList());

        if (!unmatchedGradients.isEmpty()) {
            result.append("Found gradients but no data for: ");
            unmatchedGradients.forEach(entry -> result
                .append(System.lineSeparator())
                .append(entry.getKey())
                .append(entry.getValue())
            );
        }
    }

    static class BackPropTask {
        Variable<?> variable;
        Variable<?> child;

        BackPropTask(Variable<?> variable, Variable<?> child) {
            this.variable = variable;
            this.child = child;
        }
    }

    private static final class PassThroughVariable<T extends Tensor<T>> extends SingleParentVariable<T, T> {

        private PassThroughVariable(Variable<T> parent) {
            super(parent, parent.dimensions());

            if (parent instanceof PassThroughVariable) {
                throw new IllegalArgumentException("Redundant use of PassthroughVariables. Chaining does not make sense.");
            }
        }

        @Override
        public T apply(ComputationContext ctx) {
            return ctx.data(parent);
        }

        @Override
        public T gradientForParent(ComputationContext ctx) {
            // initialize gradient computation with `1`
            return ctx.data(parent).map(v -> 1);
        }
    }
}
