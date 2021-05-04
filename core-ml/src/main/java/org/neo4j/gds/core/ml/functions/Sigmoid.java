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
package org.neo4j.gds.core.ml.functions;

import org.neo4j.gds.core.ml.ComputationContext;
import org.neo4j.gds.core.ml.Variable;
import org.neo4j.gds.core.ml.tensor.Tensor;

public class Sigmoid<T extends Tensor<T>> extends SingleParentVariable<T> {

    public Sigmoid(Variable<T> parent) {
        super(parent, parent.dimensions());
    }

    @Override
    public T apply(ComputationContext ctx) {
        return (T) ctx.data(parent()).map(Sigmoid::sigmoid);
    }

    @Override
    public T gradient(Variable<?> contextParent, ComputationContext ctx) {
        return (T) ctx.gradient(this).elementwiseProduct(ctx.data(this).map(value -> value * (1 - value)));
    }

    public static double sigmoid(double x) {
        return 1 / (1 + Math.pow(Math.E, -1 * x));
    }
}
