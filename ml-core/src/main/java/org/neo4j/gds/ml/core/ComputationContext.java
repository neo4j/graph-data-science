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

import org.neo4j.gds.ml.core.functions.PassthroughVariable;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.TensorFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ComputationContext {
    private final Map<Variable<?>, Tensor<?>> data;
    private final Map<Variable<?>, Tensor<?>> gradients;

    public ComputationContext() {
        this.data = new ConcurrentHashMap<>();
        this.gradients = new ConcurrentHashMap<>();
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
        assert (function.dimensions().length == 1 && data(function).totalSize() == 1) : "Root variable must be scalar.";
        assert function.requireGradient() : "Root variable must have requireGradient==true";

        gradients.clear();
        Queue<BackPropTask> executionQueue = new LinkedBlockingQueue<>();
        PassthroughVariable<?> dummy = new PassthroughVariable<>(function);
        executionQueue.add(new BackPropTask(function, dummy));
        Map<Variable<?>, AtomicInteger> upstreamCounters = new HashMap<>();
        initUpstream(dummy, upstreamCounters);
        backward(executionQueue, upstreamCounters);
    }

    private void backward(Queue<BackPropTask> executionQueue, Map<Variable<?>, AtomicInteger> upstreamCounters) {
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

    private void initUpstream(Variable<?> function, Map<Variable<?>, AtomicInteger> upstreamCounters) {
        for (Variable<?> parent : function.parents()) {
            if (parent.requireGradient()) {
                boolean firstToSeeParent = !upstreamCounters.containsKey(parent);
                if (firstToSeeParent) {
                    initUpstream(parent, upstreamCounters);
                    upstreamCounters.put(parent, new AtomicInteger(0));
                }
                upstreamCounters.get(parent).incrementAndGet();
            }
        }
    }

    private void updateGradient(Variable<?> variable, Tensor<?> gradient) {
        gradients.putIfAbsent(variable, TensorFactory.constant(0D, variable.dimensions()));
        gradients.get(variable).addInPlace(gradient);
    }

    static class BackPropTask {
        Variable<?> variable;
        Variable<?> child;

        BackPropTask(Variable<?> variable, Variable<?> child) {
            this.variable = variable;
            this.child = child;
        }
    }

}
