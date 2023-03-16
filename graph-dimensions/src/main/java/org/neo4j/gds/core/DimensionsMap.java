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
package org.neo4j.gds.core;

import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;

/**
 * Class that holds node property dimensions.
 * It is null-safe and will never return a null value.
 */
public final class DimensionsMap {

    private final Map<String, Optional<Integer>> actualDimensions;

    public DimensionsMap(Map<String, Optional<Integer>> actualDimensions) {
        this.actualDimensions = actualDimensions;
    }

    /**
     * Returns the dimension for the specified property, or Optional empty if no information exists.
     * <p>
     * There are two cases when empty will be returned:
     *     1) The property doesn't exist.
     *     2) The property exists, but dimension information is not known.
     */
    @NotNull
    public Optional<Integer> get(String propertyKey) {
        var dimensionOrNull = actualDimensions.get(propertyKey);
        if (dimensionOrNull == null) {
            return Optional.empty();
        }
        return dimensionOrNull;
    }
}
