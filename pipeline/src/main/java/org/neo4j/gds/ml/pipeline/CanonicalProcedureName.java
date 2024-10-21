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

import org.apache.commons.lang3.StringUtils;

import java.util.Locale;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class CanonicalProcedureName {
    private final String normalised;
    private final String raw;

    private CanonicalProcedureName(String normalised, String raw) {
        this.normalised = normalised;
        this.raw = raw;
    }

    /**
     * Canonical form: gds.shortestpath.dijkstra (notice the lower case p in shortestpath)
     * Can come from:
     * <ul>
     *     <li>gds.shortestPath.dijkstra.mutate</li>
     *     <li>GDS.SHORTESTPATH.DIJKSTRA</li>
     *     <li>shortestPath.dijkstra</li>
     *     <li>gds.shortestPath.dijkstra</li>
     * </ul>
     */
    public static CanonicalProcedureName parse(String rawInput) {
        var normalisedInput = rawInput.toLowerCase(Locale.ROOT);
        normalisedInput = !normalisedInput.startsWith("gds.") ? formatWithLocale("gds.%s", normalisedInput) : normalisedInput;
        normalisedInput = !normalisedInput.endsWith(".mutate") ? formatWithLocale("%s.mutate", normalisedInput) : normalisedInput;

        normalisedInput= StringUtils.replaceOnceIgnoreCase(normalisedInput, "beta.", "");
        normalisedInput= StringUtils.replaceOnceIgnoreCase(normalisedInput, "alpha.", "");


        return new CanonicalProcedureName(normalisedInput.substring(0, normalisedInput.length() - ".mutate".length()), rawInput);
    }

    public String getNormalisedForm() {
        return normalised;
    }

    public String getRawForm() {
        return raw;
    }

    @Override
    public int hashCode() {
        return normalised.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other instanceof CanonicalProcedureName otherCanonicalProcedureName) {
            return normalised.equals(otherCanonicalProcedureName.normalised);
        }
        return false;
    }
}
