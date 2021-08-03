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
package org.neo4j.gds.ml.core.helper;

import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.SingleParentVariable;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;

import static org.neo4j.gds.math.L2Norm.l2Norm;

public class L2Norm extends SingleParentVariable<Scalar> {
    public L2Norm(Variable<?> parent) {
        super(parent, Dimensions.scalar());
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        return new Scalar(l2Norm(ctx.data(parent()).data()));
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        var gradientValue = ctx.gradient(this).value();
        var dataValue = ctx.data(this).value();
        return ctx.data(parent).scalarMultiply(gradientValue / dataValue);
    }
}
