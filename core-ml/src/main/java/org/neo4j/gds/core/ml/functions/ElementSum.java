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

import org.neo4j.gds.core.ml.AbstractVariable;
import org.neo4j.gds.core.ml.ComputationContext;
import org.neo4j.gds.core.ml.Dimensions;
import org.neo4j.gds.core.ml.Variable;
import org.neo4j.gds.core.ml.tensor.Scalar;
import org.neo4j.gds.core.ml.tensor.Tensor;

import java.util.List;

public class ElementSum extends AbstractVariable<Scalar> {

    public ElementSum(List<Variable<?>> parents) {
        super(parents, Dimensions.scalar());
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        double sum = 0;
        for (var parent : parents()) {
            sum += ctx.data(parent).aggregateSum();
        }
        return new Scalar(sum);
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        return ctx.data(parent).map(ignore -> ctx.gradient(this).value());
    }
}
