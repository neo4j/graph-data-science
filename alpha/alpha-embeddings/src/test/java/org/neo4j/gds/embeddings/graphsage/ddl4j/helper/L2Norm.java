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
package org.neo4j.gds.embeddings.graphsage.ddl4j.helper;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.SingleParentVariable;

import static org.neo4j.graphalgo.math.L2Norm.l2Norm;

public class L2Norm extends SingleParentVariable {
    public L2Norm(Variable parent) {
        super(parent, Dimensions.scalar());
    }

    @Override
    public Tensor apply(ComputationContext ctx) {
        return Tensor.scalar(l2Norm(ctx.data(parent()).data()));
    }

    @Override
    public Tensor gradient(Variable parent, ComputationContext ctx) {
        return ctx.data(parent).scalarMultiply(ctx.gradient(this).dataAt(0) / ctx.data(this).dataAt(0));
    }
}
