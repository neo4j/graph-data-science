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
package org.neo4j.gds.ml.core;

import org.neo4j.gds.ml.core.tensor.Tensor;

import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class AbstractVariable<T extends Tensor<T>> implements Variable<T> {
    private final int[] dimensions;
    private final boolean requireGradient;
    private final List<? extends Variable<?>> parents;

    protected AbstractVariable(List<? extends Variable<?>> parents, int[] dimensions) {
        this.dimensions = dimensions;
        this.parents = parents;
        this.requireGradient = anyParentRequiresGradient();
    }

    @Override
    public List<? extends Variable<?>> parents() {
        return parents;
    }

    @Override
    public int[] dimensions() {
        return dimensions;
    }

    public int dimension(int dimensionIndex) {
        return dimensions[dimensionIndex];
    }

    @Override
    public boolean requireGradient() {
        return requireGradient;
    }

    private boolean anyParentRequiresGradient() {
        boolean parentRequiresGradient = false;
        for (Variable<?> parent : parents) {
            parentRequiresGradient |= parent.requireGradient();
        }
        return parentRequiresGradient;
    }

    public static class NotAFunctionException extends RuntimeException {

    }

    @Override
    public String toString() {
        return formatWithLocale(
            "%s: %s, requireGradient: %b",
            this.getClass().getSimpleName(),
            Dimensions.render(dimensions),
            requireGradient
        );
    }
}
