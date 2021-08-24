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

import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

public final class StringSimilarity {

    private static final double REQUIRED_SIMILARITY = 0.8;

    private static final double MAX_SCORE = 1.0;
    private static final double MIN_SCORE = 0.0;
    private static final double WINKLER_SCALING = 0.1;
    private static final int MAX_PREFIX_LENGTH_BOOST = 4;

    public static String prettySuggestions(String prefix, CharSequence value, Collection<String> candidates) {
        var result = new StringBuilder(prefix);

        var similarCandidates = StringSimilarity.similarStrings(value, candidates);
        var suffix =  Optional.ofNullable(similarCandidates.isEmpty()
            ? null
            : similarCandidates.size() == 1
                ? String.format(Locale.US, "Did you mean `%s`?", similarCandidates.get(0))
                : String.format(
                    Locale.US,
                    "Did you mean one of %s?",
                    similarCandidates.stream().collect(Collectors.joining("`, `", "[`", "`]"))
                ));

        suffix.ifPresent(prettySuggestion -> result
            .append(' ')
            .append(prettySuggestion));

        return result.toString();
    }

    public static List<String> similarStrings(CharSequence value, Collection<String> candidates) {
        return similarStrings(value, candidates, CASE_SENSITIVE);
    }

    public static List<String> similarStringsIgnoreCase(CharSequence value, Collection<String> candidates) {
        return similarStrings(value, candidates, CASE_INSENSITIVE);
    }

    private static List<String> similarStrings(
        CharSequence value,
        Collection<String> candidates,
        CharConverter converter
    ) {
        return candidates.stream()
            .map(candidate -> ImmutableStringAndScore.of(candidate, jaroWinkler(value, candidate, converter)))
            .filter(candidate -> candidate.value() > REQUIRED_SIMILARITY)
            .sorted()
            .map(ConfigKeyValidation.StringAndScore::string)
            .collect(Collectors.toList());
    }

    @FunctionalInterface
    private interface CharConverter {
        char convert(char c);
    }

    private static final CharConverter CASE_SENSITIVE = c -> c;
    private static final CharConverter CASE_INSENSITIVE = c -> Character.toLowerCase(Character.toUpperCase(c));

    @TestOnly
    static double jaro(CharSequence s1, CharSequence s2) {
        return jaro(s1, s2, CASE_SENSITIVE);
    }

    private static double jaro(CharSequence s1, CharSequence s2, CharConverter converter) {
        int len1 = s1.length();
        int len2 = s2.length();

        if (len1 == 0 && len2 == 0) {
            return MAX_SCORE;
        }
        if (len1 == 0 || len2 == 0) {
            return MIN_SCORE;
        }
        if (len1 == 1 && len2 == 1) {
            return converter.convert(s1.charAt(0)) == converter.convert(s2.charAt(0)) ? MAX_SCORE : MIN_SCORE;
        }

        int searchRange = (Math.max(len1, len2) / 2) - 1;
        boolean[] consumed2 = new boolean[len2];
        Arrays.fill(consumed2, false);

        int numberOfMatches = 0;
        int numberOfTranspositions = 0;
        int matchIndex2 = 0;

        for (int i = 0; i < len1; i++) {
            char ch1 = converter.convert(s1.charAt(i));

            int minBound = i > searchRange ? Math.max(0, i - searchRange) : 0;
            int maxBound = Math.min(len2 - 1, i + searchRange);
            if (minBound > maxBound) {
                continue;
            }

            for (int j = minBound; j <= maxBound; j++) {
                char ch2 = converter.convert(s2.charAt(j));

                if (ch1 == ch2 && !consumed2[j]) {
                    consumed2[j] = true;
                    numberOfMatches += 1;

                    if (j < matchIndex2) {
                        numberOfTranspositions += 1;
                    }
                    matchIndex2 = j;

                    break;
                }
            }
        }

        if (numberOfMatches == 0) {
            return MIN_SCORE;
        }

        double matches = numberOfMatches;
        return ((matches / len1) + (matches / len2) + ((matches - numberOfTranspositions) / matches)) / 3.0;
    }

    @TestOnly
    static double jaroWinkler(CharSequence s1, CharSequence s2) {
        return jaroWinkler(s1, s2, CASE_SENSITIVE);
    }

    private static double jaroWinkler(CharSequence s1, CharSequence s2, CharConverter converter) {
        double jaro = jaro(s1, s2, converter);
        int commonLength = Math.min(s1.length(), s2.length());
        commonLength = Math.min(MAX_PREFIX_LENGTH_BOOST + 1, commonLength);
        int prefixLength;
        for (prefixLength = 0; prefixLength < commonLength; prefixLength++) {
            char ch1 = converter.convert(s1.charAt(prefixLength));
            char ch2 = converter.convert(s2.charAt(prefixLength));
            if (ch1 != ch2) {
                break;
            }
        }

        double jaroWinkler = jaro + (WINKLER_SCALING * prefixLength * (1.0 - jaro));
        return Math.min(jaroWinkler, MAX_SCORE);
    }

    private StringSimilarity() {
        throw new UnsupportedOperationException("No instances");
    }
}
