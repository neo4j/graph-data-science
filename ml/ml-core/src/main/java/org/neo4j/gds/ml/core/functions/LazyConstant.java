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

import org.neo4j.gds.ml.core.AbstractVariable;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Tensor;

import java.util.List;
import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LazyConstant<T extends Tensor<T>> extends AbstractVariable<T> {
    private final Supplier<T> dataProducer;

    public LazyConstant(Supplier<T> dataProducer, int[] expectedDimensions) {
        super(List.of(), expectedDimensions);
        this.dataProducer = dataProducer;
    }

    @Override
    public T apply(ComputationContext ctx) {
        return dataProducer.get();
    }

    @Override
    public T gradient(Variable<?> parent, ComputationContext ctx) {
        throw new NotAFunctionException();
    }

    @Override
    public String toString() {
        return formatWithLocale(
            "%s: %s",
            this.getClass().getSimpleName(),
            dataProducer.toString()
        );
    }
}
