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
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;

public class Sigmoid extends SingleParentVariable implements Matrix {

    public Sigmoid(Variable parent) {
        super(parent, parent.dimensions());
    }

    @Override
    public Tensor apply(ComputationContext ctx) {
        return ctx.data(parent()).map(Sigmoid::sigmoid);
    }

    @Override
    public Tensor gradient(Variable contextParent, ComputationContext ctx) {
        return ctx.gradient(this).elementwiseProduct(ctx.data(this).map(value -> value * (1 - value)));
    }

    public static double sigmoid(double x) {
        return 1 / (1 + Math.pow(Math.E, -1 * x));
    }

    @Override
    public int rows() {
        return parent().dimension(0);
    }

    @Override
    public int cols() {
        return parent().dimension(1);
    }
}
