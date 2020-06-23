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
package org.neo4j.gds.embeddings.graphsage.ddl4j;

import java.util.List;

public abstract class Variable {
    private final int[] dimensions;
    protected boolean requireGradient;

    protected List<Variable> parents;

    protected Variable(List<Variable> parents, int[] dimensions) {
        this.dimensions = dimensions;
        this.parents = parents;

        this.requireGradient = false;
        for (Variable parent : parents) {
            if (parent.requireGradient) {
                this.requireGradient = true;
            }
        }
    }

    public int[] dimensions() {
        return dimensions;
    }

    public int dimension(int dimensionIndex) {
        return dimensions[dimensionIndex];
    }

    protected abstract Tensor apply(ComputationContext ctx);

    // Do not use directly. Use ComputationContext instead.
    protected abstract Tensor gradient(Variable parent, ComputationContext ctx);

    public static class NotAFunctionException extends RuntimeException {

    }
}
