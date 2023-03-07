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
package org.neo4j.gds.core.loading;

import java.util.Locale;

public final class LoadingExceptions {
    private LoadingExceptions() {}

    public static void validateTargetNodeIsLoaded(long mappedId, long neoId) {
        validateNodeIsLoaded(mappedId, neoId, "target");
    }

    public static void validateSourceNodeIsLoaded(long mappedId, long neoId) {
        validateNodeIsLoaded(mappedId, neoId, "source");
    }

    private static void validateNodeIsLoaded(long mappedId, long neoId, String side) {
        if (mappedId == -1) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.US,
                    "Failed to load a relationship because its %s-node with id %s is not part of the node query or projection. " +
                    "To ignore the relationship, set the configuration parameter `validateRelationships` to false.",
                    side,
                    neoId
                )
            );
        }
    }
}
