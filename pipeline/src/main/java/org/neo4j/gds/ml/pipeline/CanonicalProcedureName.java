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
package org.neo4j.gds.ml.pipeline;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Locale;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class CanonicalProcedureName {
    private final String value;

    private CanonicalProcedureName(String value) {
        this.value = value;
    }

    /**
     * Canonical form: gds.shortestpath.dijkstra.mutate (notice the lower case p in shortestpath)
     * Can come from:
     * <ul>
     *     <li>gds.shortestPath.dijkstra.mutate</li>
     *     <li>GDS.SHORTESTPATH.DIJKSTRA</li>
     *     <li>shortestPath.dijkstra</li>
     *     <li>gds.shortestPath.dijkstra</li>
     * </ul>
     */
    static CanonicalProcedureName parse(String input) {
        input = input.toLowerCase(Locale.ROOT);
        input = !input.startsWith("gds.") ? formatWithLocale("gds.%s", input) : input;
        input = !input.endsWith(".mutate") ? formatWithLocale("%s.mutate", input) : input;

        return new CanonicalProcedureName(input);
    }

    String getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
}
