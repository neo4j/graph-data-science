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
package org.neo4j.graphalgo.core;

import org.immutables.value.Value;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.neo4j.graphalgo.core.StringSimilarity.jaroWinkler;

/**
 * Wrapper around configuration options map
 */
public final class CypherMapWrapper {

    private static final double REQUIRED_SIMILARITY = 0.8;

    private final Map<String, Object> config;

    private CypherMapWrapper(Map<String, Object> config) {
        this.config = config;
    }

    /**
     * Checks if the given key exists in the configuration.
     *
     * @param key key to look for
     * @return true, iff the key exists
     */
    public boolean containsKey(String key) {
        return this.config.containsKey(key);
    }

    public boolean isEmpty() {
        return config.isEmpty();
    }

    public Optional<String> getString(String key) {
        return Optional.ofNullable(getChecked(key, null, String.class));
    }

    public String requireString(String key) {
        return requireChecked(key, String.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String key) {
        return getChecked(key, emptyMap(), Map.class);
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(String key) {
        return getChecked(key, emptyList(), List.class);
    }

    @Contract("_, !null -> !null")
    public @Nullable String getString(String key, @Nullable String defaultValue) {
        return getChecked(key, defaultValue, String.class);
    }

    @Contract("_, _, !null -> !null")
    public @Nullable String getString(String key, String oldKey, @Nullable String defaultValue) {
        String value = getChecked(key, null, String.class);
        if (value != null) {
            return value;
        }
        return getChecked(oldKey, defaultValue, String.class);
    }

    Optional<String> getStringWithFallback(String key, String oldKey) {
        Optional<String> value = getString(key);
        // #migration-note: On Java9+ there is a #or method on Optional that we should use instead
        //  https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Optional.html#or(java.util.function.Supplier)
        if (!value.isPresent()) {
            value = getString(oldKey);
        }
        return value;
    }

    public boolean getBool(String key, boolean defaultValue) {
        return getChecked(key, defaultValue, Boolean.class);
    }

    public boolean requireBool(String key) {
        return requireChecked(key, Boolean.class);
    }

    public Number getNumber(String key, Number defaultValue) {
        return getChecked(key, defaultValue, Number.class);
    }

    public Number requireNumber(String key) {
        return requireChecked(key, Number.class);
    }

    public Number getNumber(String key, String oldKey, Number defaultValue) {
        Number value = getChecked(key, null, Number.class);
        if (value != null) {
            return value;
        }
        return getChecked(oldKey, defaultValue, Number.class);
    }

    public long getLong(String key, long defaultValue) {
        return getChecked(key, defaultValue, Long.class);
    }

    public long requireLong(String key) {
        return requireChecked(key, Long.class);
    }

    public int getInt(String key, int defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return getLongAsInt(key);
    }

    public int requireInt(String key) {
        if (!containsKey(key)) {
            throw missingValueFor(key);
        }
        return getLongAsInt(key);
    }

    private int getLongAsInt(String key) {
        Object value = config.get(key);
        // Cypher always uses longs, so we have to downcast them to ints
        if (value instanceof Long) {
            value = Math.toIntExact((Long) value);
        }
        return typedValue(key, Integer.class, value);
    }

    public double getDouble(String key, double defaultValue) {
        return getChecked(key, defaultValue, Double.class);
    }

    /**
     * Returns a copy of the internal Map.
     */
    public Map<String, Object> toMap() {
        return new HashMap<>(config);
    }

    public double requireDouble(String key) {
        return requireChecked(key, Double.class);
    }

    /**
     * Get and convert the value under the given key to the given type.
     *
     * @return the found value under the key - if it is of the provided type,
     *     or the provided default value if no entry for the key is found (or it's mapped to null).
     * @throws IllegalArgumentException if a value was found, but it is not of the expected type.
     */
    @Contract("_, !null, _ -> !null")
    public @Nullable <V> V getChecked(String key, @Nullable V defaultValue, Class<V> expectedType) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return typedValue(key, expectedType, config.get(key));
    }

    public <V> V requireChecked(String key, Class<V> expectedType) {
        if (!containsKey(key)) {
            throw missingValueFor(key);
        }
        return typedValue(key, expectedType, config.get(key));
    }

    public void requireOnlyKeysFrom(Collection<String> allowedKeys) {
        Collection<String> keys = new HashSet<>(config.keySet());
        keys.removeAll(allowedKeys);
        if (keys.isEmpty()) {
            return;
        }
        List<String> suggestions = keys.stream()
            .map(invalid -> {
                List<String> candidates = similarStrings(invalid, allowedKeys);
                if (candidates.isEmpty()) {
                    return invalid;
                }
                if (candidates.size() == 1) {
                    return String.format("%s (Did you mean [%s]?)", invalid, candidates.get(0));
                }
                return String.format("%s (Did you mean one of [%s]?)", invalid, String.join(", ", candidates));
            })
            .collect(Collectors.toList());

        if (suggestions.size() == 1) {
            throw new IllegalArgumentException(String.format(
                "Unexpected configuration key: %s",
                suggestions.get(0)
            ));
        }

        throw new IllegalArgumentException(String.format(
            "Unexpected configuration keys: %s",
            String.join(", ", suggestions)
        ));
    }

    @SuppressWarnings("unchecked")
    @Contract("_, !null -> !null")
    @Deprecated
    public <V> @Nullable V get(String key, @Nullable V defaultValue) {
        Object value = config.get(key);
        if (null == value) {
            return defaultValue;
        }
        return (V) value;
    }

    @SuppressWarnings("unchecked")
    @Contract("_, _, !null -> !null")
    @Deprecated
    public <V> @Nullable V get(String newKey, String oldKey, @Nullable V defaultValue) {
        Object value = config.get(newKey);
        if (null == value) {
            value = config.get(oldKey);
        }
        return null == value ? defaultValue : (V) value;
    }

    public static <T> T failOnNull(String key, T value) {
        if (value == null) {
            throw missingValueFor(key, Collections.emptySet());
        }
        return value;
    }

    public static String failOnBlank(String key, String value) {
        if (value == null || value.trim().isEmpty()) {
            throw blankValueFor(key, value);
        }
        return value;
    }

    public static int validateIntegerRange(
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
            throw outOfRangeError(key, Integer.toString(min), Integer.toString(max), minInclusive, maxInclusive);
        }

        return value;
    }

    public static double validateDoubleRange(
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
                String.format(Locale.ENGLISH, "%.2f", min),
                String.format(Locale.ENGLISH, "%.2f", max),
                minInclusive,
                maxInclusive
            );
        }

        return value;
    }

    static <V> V typedValue(String key, Class<V> expectedType, @Nullable Object value) {
        if (canHardCastToDouble(expectedType, value)) {
            return expectedType.cast(((Number) value).doubleValue());
        } else if (!expectedType.isInstance(value)) {
            String message = String.format(
                "The value of `%s` must be of type `%s` but was `%s`.",
                key,
                expectedType.getSimpleName(),
                value == null ? "null" : value.getClass().getSimpleName()
            );
            throw new IllegalArgumentException(message);
        }
        return expectedType.cast(value);
    }

    private static boolean canHardCastToDouble(Class<?> expectedType, @Nullable Object value) {
        return Double.class.isAssignableFrom(expectedType) && Number.class.isInstance(value);
    }

    private IllegalArgumentException missingValueFor(String key) {
        return missingValueFor(key, config.keySet());
    }

    private static IllegalArgumentException missingValueFor(String key, Collection<String> candidates) {
        return new IllegalArgumentException(missingValueForMessage(key, candidates));
    }

    private static String missingValueForMessage(String key, Collection<String> candidates) {
        List<String> suggestions = similarStrings(key, candidates);
        return missingValueMessage(key, suggestions);
    }

    private static String missingValueMessage(String key, List<String> suggestions) {
        if (suggestions.isEmpty()) {
            return String.format(
                "No value specified for the mandatory configuration parameter `%s`",
                key
            );
        }
        if (suggestions.size() == 1) {
            return String.format(
                "No value specified for the mandatory configuration parameter `%s` (a similar parameter exists: [%s])",
                key,
                suggestions.get(0)
            );
        }
        return String.format(
            "No value specified for the mandatory configuration parameter `%s` (similar parameters exist: [%s])",
            key,
            String.join(", ", suggestions)
        );
    }

    public enum PairResult {
        FIRST_PAIR,
        SECOND_PAIR,
    }

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
    public PairResult verifyMutuallyExclusivePairs(
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
        throw new IllegalArgumentException(String.format("%s %s", errorPrefix, message));
    }

    private boolean checkMutuallyExclusivePairs(
        String firstPairKeyOne,
        String firstPairKeyTwo,
        String secondPairKeyOne,
        String secondPairKeyTwo
    ) throws IllegalArgumentException {
        if (config.containsKey(firstPairKeyOne) && config.containsKey(firstPairKeyTwo)) {
            boolean secondOneExists = config.containsKey(secondPairKeyOne);
            boolean secondTwoExists = config.containsKey(secondPairKeyTwo);
            if (secondOneExists && secondTwoExists) {
                throw new IllegalArgumentException(String.format(
                    "Invalid keys: [%s, %s]. Those keys cannot be used together with `%s` and `%s`.",
                    secondPairKeyOne,
                    secondPairKeyTwo,
                    firstPairKeyOne,
                    firstPairKeyTwo
                ));
            } else if (secondOneExists || secondTwoExists) {
                throw new IllegalArgumentException(String.format(
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
        StringAndScore firstMessage = missingMutuallyExclusivePairs(firstPairKeyOne, firstPairKeyTwo, secondPairKeyOne, secondPairKeyTwo);
        StringAndScore secondMessage = missingMutuallyExclusivePairs(secondPairKeyOne, secondPairKeyTwo, firstPairKeyOne, firstPairKeyTwo);

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
            "Specify either `%s` and `%s` or `%s` and `%s`.",
            firstPairKeyOne,
            firstPairKeyTwo,
            secondPairKeyOne,
            secondPairKeyTwo
        );
    }

    private @Nullable StringAndScore missingMutuallyExclusivePairs(
        String keyOne,
        String keyTwo,
        String... forbiddenSuggestions
    ) {
        Collection<String> missingAndCandidates = new ArrayList<>();
        Collection<String> missingWithoutCandidates = new ArrayList<>();
        boolean hasAtLastOneKey = false;
        for (String key : asList(keyOne, keyTwo)) {
            if (config.containsKey(key)) {
                hasAtLastOneKey = true;
            } else {
                List<String> candidates = similarStrings(key, config.keySet());
                candidates.removeAll(asList(forbiddenSuggestions));
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

    private static IllegalArgumentException blankValueFor(String key, @Nullable String value) {
        return new IllegalArgumentException(String.format(
            "`%s` can not be null or blank, but it was `%s`",
            key,
            value
        ));
    }

    private static IllegalArgumentException outOfRangeError(
        String key,
        String min,
        String max,
        boolean minInclusive,
        boolean maxInclusive
    ) {
        return new IllegalArgumentException(String.format(
            "Value for `%s` must be within %s%s, %s%s.",
            key,
            minInclusive ? "[" : "(",
            min,
            max,
            maxInclusive ? "]" : ")"
        ));
    }

    private static List<String> similarStrings(CharSequence value, Collection<String> candidates) {
        return candidates.stream()
            .map(candidate -> ImmutableStringAndScore.of(candidate, jaroWinkler(value, candidate)))
            .filter(candidate -> candidate.value() > REQUIRED_SIMILARITY)
            .sorted()
            .map(StringAndScore::string)
            .collect(Collectors.toList());
    }

    // FACTORIES

    public static CypherMapWrapper create(Map<String, Object> config) {
        if (config == null) {
            return empty();
        }
        Map<String, Object> filteredConfig = config.entrySet()
            .stream()
            .filter(e -> e.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new CypherMapWrapper(filteredConfig);
    }

    public static CypherMapWrapper empty() {
        return new CypherMapWrapper(emptyMap());
    }

    public CypherMapWrapper withString(String key, String value) {
        return withEntry(key, value);
    }

    public CypherMapWrapper withNumber(String key, Number value) {
        return withEntry(key, value);
    }

    public CypherMapWrapper withBoolean(String key, Boolean value) {
        return withEntry(key, value);
    }

    public CypherMapWrapper withEntry(String key, Object value) {
        HashMap<String, Object> newMap = new HashMap<>(config);
        newMap.put(key, value);
        return new CypherMapWrapper(newMap);
    }

    CypherMapWrapper withDouble(String key, Double value) {
        HashMap<String, Object> newMap = new HashMap<>(config);
        newMap.put(key, value);
        return new CypherMapWrapper(newMap);
    }

    public CypherMapWrapper withoutEntry(String key) {
        if (!containsKey(key)) {
            return this;
        }
        HashMap<String, Object> newMap = new HashMap<>(config);
        newMap.remove(key);
        return new CypherMapWrapper(newMap);
    }

    public CypherMapWrapper withoutAny(Collection<String> keys) {
        Map<String, Object> newMap = new HashMap<>(config);
        newMap.keySet().removeAll(keys);
        return new CypherMapWrapper(newMap);
    }

    @Value.Style(
        allParameters = true,
        builderVisibility = Value.Style.BuilderVisibility.SAME,
        jdkOnly = true,
        overshadowImplementation = true,
        typeAbstract = "*",
        visibility = Value.Style.ImplementationVisibility.PACKAGE
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
