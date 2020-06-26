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

import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class InnerProduct extends AbstractVariable {
    private final Variable left;
    private final Variable right;

    public InnerProduct(Variable left, Variable right) {
        super(List.of(left, right), Dimensions.scalar());
        this.left = left;
        this.right = right;
        checkDimensions();
    }

    private void checkDimensions() {
        if (left.dimension(0) != right.dimension(0)) {
            throw new IllegalArgumentException(formatWithLocale("Dimensions of left: %d do not match dimensions of right: %d",
                left.dimension(0),
                right.dimension(0)
            ));
        }
    }

    @Override
    public Tensor apply(ComputationContext ctx) {
        return Tensor.scalar(ctx.data(left).innerProduct(ctx.data(right)));
    }

    @Override
    public Tensor gradient(Variable parent, ComputationContext ctx) {
        Tensor otherVectorData = parent == left ? ctx.data(right) : ctx.data(left);
        return otherVectorData.scalarMultiply(ctx.gradient(this).data[0]);
    }
}
