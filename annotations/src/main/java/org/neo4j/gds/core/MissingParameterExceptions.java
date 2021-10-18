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

import java.util.Collection;
import java.util.List;
import java.util.Locale;

public final class MissingParameterExceptions {
    public static IllegalArgumentException missingValueFor(String key, Collection<String> candidates) {
        return new IllegalArgumentException(missingValueForMessage(key, candidates));
    }

    public static String missingValueForMessage(String key, Collection<String> candidates) {
        List<String> suggestions = StringSimilarity.similarStringsIgnoreCase(key, candidates);
        return missingValueMessage(key, suggestions);
    }

    static String missingValueMessage(String key, List<String> suggestions) {
        if (suggestions.isEmpty()) {
            return String.format(
                Locale.US,
                "No value specified for the mandatory configuration parameter `%s`",
                key
            );
        }
        if (suggestions.size() == 1) {
            return String.format(
                Locale.ENGLISH,
                "No value specified for the mandatory configuration parameter `%s` (a similar parameter exists: [%s])",
                key,
                suggestions.get(0)
            );
        }
        return String.format(
            Locale.ENGLISH,
            "No value specified for the mandatory configuration parameter `%s` (similar parameters exist: [%s])",
            key,
            String.join(", ", suggestions)
        );
    }
}
