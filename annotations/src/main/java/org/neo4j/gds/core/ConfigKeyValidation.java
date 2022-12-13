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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class ConfigKeyValidation {

    private ConfigKeyValidation() {}

    public static void requireOnlyKeysFrom(Collection<String> allowedKeys, Iterable<String> configKeys) {
        var unexpectedKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (configKeys instanceof Collection<?>) {
            unexpectedKeys.addAll((Collection<String>) configKeys);
        } else {
            configKeys.forEach(unexpectedKeys::add);
        }


        // We are doing the equivalent of `removeAll` here.
        // As of jdk16, TreeSet does not have a specialized removeAll implementation and reverts to use
        // the default implementation in `AbstractSet#removeAll`
        // The JavaDocs for this method specifies the following behavior:
        // > This implementation determines which is the smaller of this set and the specified collection [..]
        // > If this set has fewer elements, then the implementation iterates over this set, checking
        // > each element [..] to see if it is contained in the specified collection.
        // The specified collection does not guarantee the case-insensitive comparison and
        // `removeAll` would not return the correct result.
        for (var allowedKey : allowedKeys) {
            unexpectedKeys.remove(allowedKey);
        }
        if (unexpectedKeys.isEmpty()) {
            return;
        }
        List<String> suggestions = unexpectedKeys.stream()
            .map(invalid -> {
                List<String> candidates = StringSimilarity.similarStringsIgnoreCase(invalid, allowedKeys);
                if (configKeys instanceof Collection<?>) {
                    candidates.removeAll((Collection<String>) configKeys);
                } else {
                    configKeys.forEach(candidates::remove);
                }


                if (candidates.isEmpty()) {
                    return invalid;
                }
                if (candidates.size() == 1) {
                    return String.format(Locale.ENGLISH, "%s (Did you mean [%s]?)", invalid, candidates.get(0));
                }
                return String.format(
                    Locale.ENGLISH,
                    "%s (Did you mean one of [%s]?)",
                    invalid,
                    String.join(", ", candidates)
                );
            })
            .collect(Collectors.toList());

        if (suggestions.size() == 1) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Unexpected configuration key: %s",
                suggestions.get(0)
            ));
        }

        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "Unexpected configuration keys: %s",
            String.join(", ", suggestions)
        ));
    }

    @Value.Style(
        allParameters = true,
        builderVisibility = Value.Style.BuilderVisibility.SAME,
        jdkOnly = true,
        overshadowImplementation = true,
        typeAbstract = "*",
        visibility = Value.Style.ImplementationVisibility.PUBLIC
    )
    @Value.Immutable(copy = false, builder = false)
    interface StringAndScore extends Comparable<StringAndScore> {
        String string();

        double value();

        default boolean isBetterThan(@Nullable StringAndScore other) {
            return other == null || value() > other.value();
        }

        @Override
        default int compareTo(StringAndScore other) {
            // ORDER BY score DESC, string ASC
            int result = Double.compare(other.value(), this.value());
            return (result != 0) ? result : this.string().compareTo(other.string());
        }
    }
}
