/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.AbstractVariable;

import java.util.Arrays;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class InvertScalar extends SingleParentVariable {
    InvertScalar(AbstractVariable parent) {
        super(parent, Dimensions.scalar());
        checkParentScalar();
    }

    @Override
    public Tensor apply(ComputationContext ctx) {
        return Tensor.scalar(1D / ctx.data(parent()).data[0]);
    }

    @Override
    public Tensor gradient(Variable parent, ComputationContext ctx) {
        return ctx.gradient(this).scalarMultiply(-Math.pow(ctx.data(parent).data[0], -2));
    }

    private void checkParentScalar() {
        int[] parentDimensions = parent().dimensions();
        if (parentDimensions.length != 1 || parentDimensions[0] != 1) {
            throw new IllegalArgumentException(formatWithLocale(
                "Parent is expected to be scalar but has dimension %s",
                Arrays.toString(parentDimensions)
            ));
        }
    }
}
