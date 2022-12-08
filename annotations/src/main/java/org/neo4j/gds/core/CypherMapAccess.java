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

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.core.MissingParameterExceptions.missingValueMessage;

public interface CypherMapAccess {
    static <T> T failOnNull(String key, T value) {
        if (value == null) {
            throw MissingParameterExceptions.missingValueFor(key, Collections.emptySet());
        }
        return value;
    }

    static @NotNull String failOnBlank(String key, @Nullable String value) {
        if (value == null || value.trim().isEmpty()) {
            throw blankValueFor(key, value);
        }
        return value;
    }

    private static IllegalArgumentException blankValueFor(String key, @Nullable String value) {
        return new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "`%s` can not be null or blank, but it was `%s`",
            key,
            value
        ));
    }

    private static IllegalArgumentException outOfRangeError(
        String key,
        Number value,
        String min,
        String max,
        boolean minInclusive,
        boolean maxInclusive
    ) {
        return new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "Value for `%s` was `%s`, but must be within the range %s%s, %s%s.",
            key,
            value,
            minInclusive ? "[" : "(",
            min,
            max,
            maxInclusive ? "]" : ")"
        ));
    }

    static int validateIntegerRange(
        String key,
        int value,
        int min,
        int max,
        boolean minInclusive,
        boolean maxInclusive
    ) {
        boolean meetsLowerBound = minInclusive ? value >= min : value > min;
        boolean meetsUpperBound = maxInclusive ? value <= max : value < max;

        if (!meetsLowerBound || !meetsUpperBound) {
            throw outOfRangeError(
                key,
                value,
                Integer.toString(min),
                Integer.toString(max),
                minInclusive,
                maxInclusive
            );
        }

        return value;
    }

    static long validateLongRange(
        String key,
        long value,
        long min,
        long max,
        boolean minInclusive,
        boolean maxInclusive
    ) {
        boolean meetsLowerBound = minInclusive ? value >= min : value > min;
        boolean meetsUpperBound = maxInclusive ? value <= max : value < max;

        if (!meetsLowerBound || !meetsUpperBound) {
            throw outOfRangeError(
                key,
                value,
                Long.toString(min),
                Long.toString(max),
                minInclusive,
                maxInclusive
            );
        }

        return value;
    }

    static double validateDoubleRange(
        String key,
        double value,
        double min,
        double max,
        boolean minInclusive,
        boolean maxInclusive
    ) {
        boolean meetsLowerBound = minInclusive ? value >= min : value > min;
        boolean meetsUpperBound = maxInclusive ? value <= max : value < max;

        if (!meetsLowerBound || !meetsUpperBound) {
            throw outOfRangeError(
                key,
                value,
                String.format(Locale.ENGLISH, "%.2f", min),
                String.format(Locale.ENGLISH, "%.2f", max),
                minInclusive,
                maxInclusive
            );
        }

        return value;
    }

    default IllegalArgumentException missingValueFor(String key) {
        return MissingParameterExceptions.missingValueFor(key, this.keySet());
    }

    default void requireOnlyKeysFrom(Collection<String> allowedKeys) {
        ConfigKeyValidation.requireOnlyKeysFrom(allowedKeys, this.keySet());
    }

    /**
     * Checks if the given key exists in the configuration.
     *
     * @param key key to look for
     * @return true, iff the key exists
     */
    boolean containsKey(String key);

    Collection<String> keySet();

    default Optional<String> getString(String key) {
        return Optional.ofNullable(getChecked(key, null, String.class));
    }

    default String requireString(String key) {
        return requireChecked(key, String.class);
    }

    default <E> Optional<E> getOptional(String key, Class<E> clazz) {
        return Optional.ofNullable(getChecked(key, null, clazz));
    }

    @Contract("_, !null -> !null")
    default @Nullable String getString(String key, @Nullable String defaultValue) {
        return getChecked(key, defaultValue, String.class);
    }

    default boolean getBool(String key, boolean defaultValue) {
        return getChecked(key, defaultValue, Boolean.class);
    }

    default boolean requireBool(String key) {
        return requireChecked(key, Boolean.class);
    }

    default Number getNumber(String key, Number defaultValue) {
        return getChecked(key, defaultValue, Number.class);
    }

    default Number requireNumber(String key) {
        return requireChecked(key, Number.class);
    }

    default long getLong(String key, long defaultValue) {
        return getChecked(key, defaultValue, Long.class);
    }

    default long requireLong(String key) {
        return requireChecked(key, Long.class);
    }

    default int getInt(String key, int defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return getLongAsInt(key);
    }

    default int requireInt(String key) {
        if (!containsKey(key)) {
            throw missingValueFor(key);
        }
        return getLongAsInt(key);
    }

    int getLongAsInt(String key);

    default double getDouble(String key, double defaultValue) {
        return getChecked(key, defaultValue, Double.class);
    }

    default double requireDouble(String key) {
        return requireChecked(key, Double.class);
    }

    @Contract("_, !null, _ -> !null")
    default @Nullable <V> V getChecked(String key, @Nullable V defaultValue, Class<V> expectedType) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return typedValue(key, expectedType);
    }

    default <V> V requireChecked(String key, Class<V> expectedType) {
        if (!containsKey(key)) {
            throw missingValueFor(key);
        }
        return typedValue(key, expectedType);
    }

    @NotNull <V> V typedValue(String key, Class<V> expectedType);

    /**
     * Returns a copy of the internal Map.
     */
    Map<String, Object> toMap();

    /**
     * Verifies that only one of two mutually exclusive pairs of configuration keys is present.
     *
     * More precisely, the following condition is checked:
     *  {@code (firstPairKeyOne AND firstPairKeyTwo) XOR (secondPairKeyOne AND secondPairKeyTwo)}
     * If the condition is verified, the return value will identify which one of the pairs is present.
     *
     * In the error case where the condition is violated, an {@link IllegalArgumentException} is thrown.
     * The message of that exception depends on which keys are present, possible mis-spelled, or absent.
     */
    default PairResult verifyMutuallyExclusivePairs(
        String firstPairKeyOne,
        String firstPairKeyTwo,
        String secondPairKeyOne,
        String secondPairKeyTwo,
        String errorPrefix
    ) throws IllegalArgumentException {
        boolean isValidFirstPair = checkMutuallyExclusivePairs(
            firstPairKeyOne, firstPairKeyTwo, secondPairKeyOne, secondPairKeyTwo
        );
        if (isValidFirstPair) {
            return PairResult.FIRST_PAIR;
        }

        boolean isValidSecondPair = checkMutuallyExclusivePairs(
            secondPairKeyOne, secondPairKeyTwo, firstPairKeyOne, firstPairKeyTwo
        );
        if (isValidSecondPair) {
            return PairResult.SECOND_PAIR;
        }

        String message = missingMutuallyExclusivePairMessage(firstPairKeyOne, firstPairKeyTwo, secondPairKeyOne, secondPairKeyTwo);
        throw new IllegalArgumentException(String.format(Locale.ENGLISH,"%s %s", errorPrefix, message));
    }

    private boolean checkMutuallyExclusivePairs(
        String firstPairKeyOne,
        String firstPairKeyTwo,
        String secondPairKeyOne,
        String secondPairKeyTwo
    ) throws IllegalArgumentException {
        if (this.containsKey(firstPairKeyOne) && this.containsKey(firstPairKeyTwo)) {
            boolean secondOneExists = this.containsKey(secondPairKeyOne);
            boolean secondTwoExists = this.containsKey(secondPairKeyTwo);
            if (secondOneExists && secondTwoExists) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Invalid keys: [%s, %s]. Those keys cannot be used together with `%s` and `%s`.",
                    secondPairKeyOne,
                    secondPairKeyTwo,
                    firstPairKeyOne,
                    firstPairKeyTwo
                ));
            } else if (secondOneExists || secondTwoExists) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Invalid key: [%s]. This key cannot be used together with `%s` and `%s`.",
                    secondOneExists ? secondPairKeyOne : secondPairKeyTwo,
                    firstPairKeyOne,
                    firstPairKeyTwo
                ));
            }
            return true;
        }
        return false;
    }

    private String missingMutuallyExclusivePairMessage(
        String firstPairKeyOne,
        String firstPairKeyTwo,
        String secondPairKeyOne,
        String secondPairKeyTwo
    ) {
        ConfigKeyValidation.StringAndScore firstMessage = missingMutuallyExclusivePairs(firstPairKeyOne, firstPairKeyTwo, secondPairKeyOne, secondPairKeyTwo);
        ConfigKeyValidation.StringAndScore secondMessage = missingMutuallyExclusivePairs(secondPairKeyOne, secondPairKeyTwo, firstPairKeyOne, firstPairKeyTwo);

        if (firstMessage != null && firstMessage.isBetterThan(secondMessage)) {
            // only return if the second message does not have a competitive score
            return firstMessage.string();
        }

        if (secondMessage != null && secondMessage.isBetterThan(firstMessage)) {
            // only return if the first message does not have a competitive score
            return secondMessage.string();
        }

        // either pairs have the same possibility score, we don't know which one we should use
        return String.format(
            Locale.ENGLISH,
            "Specify either `%s` and `%s` or `%s` and `%s`.",
            firstPairKeyOne,
            firstPairKeyTwo,
            secondPairKeyOne,
            secondPairKeyTwo
        );
    }

    private @Nullable ConfigKeyValidation.StringAndScore missingMutuallyExclusivePairs(
        String keyOne,
        String keyTwo,
        String... forbiddenSuggestions
    ) {
        Collection<String> missingAndCandidates = new ArrayList<>();
        Collection<String> missingWithoutCandidates = new ArrayList<>();
        boolean hasAtLastOneKey = false;
        for (String key : List.of(keyOne, keyTwo)) {
            if (this.containsKey(key)) {
                hasAtLastOneKey = true;
            } else {
                List<String> candidates = StringSimilarity.similarStringsIgnoreCase(key, this.keySet());
                candidates.removeAll(List.of(forbiddenSuggestions));
                String message = missingValueMessage(key, candidates);
                (candidates.isEmpty()
                    ? missingWithoutCandidates
                    : missingAndCandidates
                ).add(message);
            }
        }
        // if one of the keys matches, we give it a full score,
        //   meaning "this is probably a pair that should be used"
        // if one of the keys is mis-spelled, we give it a half score,
        //   meaning "this could be that pair, but it might me something else"
        // If none of the keys are present or mis-spelled, we give it a zero score,
        //   meaning "This is not pair you are looking for" *waves hand*
        double score = hasAtLastOneKey ? 1.0 : !missingAndCandidates.isEmpty() ? 0.5 : 0.0;
        if (!missingAndCandidates.isEmpty()) {
            missingAndCandidates.addAll(missingWithoutCandidates);
            String message = String.join(". ", missingAndCandidates);
            return ImmutableStringAndScore.of(message, score);
        }
        if (hasAtLastOneKey && !missingWithoutCandidates.isEmpty()) {
            String message = String.join(". ", missingWithoutCandidates);
            return ImmutableStringAndScore.of(message, score);
        }
        // null here means, that there are no valid keys, but also no good error message
        // so it might be that this pair is not relevant for the error reporting
        return null;
    }

    enum PairResult {
        FIRST_PAIR,
        SECOND_PAIR,
    }
}
