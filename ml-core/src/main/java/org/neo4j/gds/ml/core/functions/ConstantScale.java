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
package org.neo4j.gds.ml.core.functions;

import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Tensor;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class ConstantScale<T extends Tensor<T>> extends SingleParentVariable<T, T> {
    private final double constant;

    public ConstantScale(Variable<T> parent, double constant) {
        super(parent, parent.dimensions());
        this.constant = constant;
    }

    @Override
    public T apply(ComputationContext ctx) {
        return ctx.data(parent).scalarMultiply(constant);
    }

    @Override
    protected T gradientForParent(ComputationContext ctx) {
        return ctx.gradient(this).scalarMultiply(constant);
    }

    @Override
    public String toString() {
        return formatWithLocale(
            "%s: scale by %s, requireGradient: %b",
            this.getClass().getSimpleName(),
            Double.toString(constant),
            requireGradient()
        );
    }
}
