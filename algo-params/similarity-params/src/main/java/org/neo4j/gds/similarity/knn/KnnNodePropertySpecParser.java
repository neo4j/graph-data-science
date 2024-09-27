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
package org.neo4j.gds.similarity.knn;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.similarity.knn.metrics.SimilarityMetric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class KnnNodePropertySpecParser {

    private KnnNodePropertySpecParser() {}

    /**
     * User input is one of
     *     * {@code java.lang.String}
     *     * {@code java.util.List<java.lang.String>}
     *     * {@code java.util.Map<java.lang.String, java.lang.String>}
     *
     * The single string is parsed as a property name.
     * The list of strings is parsed as a list of property names.
     * The map of string to string is parsed as a map from property names to similarity metrics.
     */
    public static List<KnnNodePropertySpec> parse(@NotNull Object userInput) {
        if (userInput instanceof String) {
            return fromMap(Collections.singletonMap((String) userInput, null));
        }
        if (userInput instanceof Iterable) {
            var data = new HashMap<String, String>();
            for (Object item : (Iterable) userInput) {
                if (item instanceof String) {
                    data.put((String) item, null);
                } else if (item instanceof Map) {
                    data.putAll(parseMap((Map<?, ?>) item));
                } else if (item instanceof KnnNodePropertySpec) {
                    var spec = ((KnnNodePropertySpec) item);
                    data.put(spec.name(), spec.metric().name());
                }else {
                    throw new IllegalArgumentException(
                        formatWithLocale(
                            "Cannot construct KnnNodePropertySpec out of %s",
                            item.getClass().getName()
                        )
                    );
                }
            }
            return fromMap(data);
        }
        if (userInput instanceof Map) {
            var data = parseMap((Map<?, ?>) userInput);
            return fromMap(data);
        }
        throw new IllegalArgumentException(
            formatWithLocale(
                "Cannot parse KnnNodePropertySpecs from %s",
                userInput.getClass().getName()
            )
        );
    }

    private static HashMap<String, String> parseMap(Map<?, ?> item) {
        var data = new HashMap<String, String>();
        for (var mapItem : item.entrySet()) {
            if (mapItem.getKey() instanceof String && mapItem.getValue() instanceof String) {
                data.put((String) mapItem.getKey(), (String) mapItem.getValue());
            } else {
                throw new IllegalArgumentException(
                    formatWithLocale(
                        "Cannot construct KnnNodePropertySpec out of %s and %s",
                        mapItem.getKey().getClass().getName(),
                        mapItem.getValue().getClass().getName()
                    )
                );
            }
        }
        return data;
    }

    private static List<KnnNodePropertySpec> fromMap(Map<String, String> userInput) {
        validatePropertyNames(userInput.keySet());
        var knnNodeProperties = new ArrayList<KnnNodePropertySpec>();
        for (var entry : userInput.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            SimilarityMetric similarityMetric;
            if (value != null) {
                try {
                    similarityMetric = SimilarityMetric.parse(value);
                    knnNodeProperties.add(new KnnNodePropertySpec(key, similarityMetric));
                } catch (IllegalArgumentException ex) {
                    throw new IllegalArgumentException(
                        formatWithLocale("No valid similarity metric for user input %s", value)
                    );
                }
            } else {
                knnNodeProperties.add(new KnnNodePropertySpec(key));
            }
        }
        return knnNodeProperties;
    }

    static @Nullable List<String> validatePropertyNames(Collection<String> input) {
        if (input.isEmpty()) {
            throw new IllegalArgumentException("The 'nodeProperties' parameter must not be empty.");
        }
        return input.stream()
            .map(KnnNodePropertySpecParser::validatePropertyName)
            .collect(Collectors.toList());
    }

    private static String validatePropertyName(String input) {
        return validateNoWhiteCharacter(emptyToNull(input), "nodeProperties");
    }

    public static Map<String, String> render(List<KnnNodePropertySpec> specs) {
        return specs.stream().collect(Collectors.toMap(KnnNodePropertySpec::name, (spec) -> spec.metric().name()));
    }
}
