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
package org.neo4j.gds.ml.models.automl;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.ml.models.automl.hyperparameter.ConcreteParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.DoubleParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.DoubleRangeParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.IntegerParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.IntegerRangeParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.ListParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.StringParameter;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.models.automl.TunableTrainerConfig.LOG_SCALE_PARAMETERS;
import static org.neo4j.gds.ml.models.automl.TunableTrainerConfig.NON_NUMERIC_PARAMETERS;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class ParameterParser {
    private ParameterParser() {}

    static RangeParameters parseRangeParameters(
        Map<String, Object> input
    ) {
        var doubleRanges = new HashMap<String, DoubleRangeParameter>();
        var integerRanges = new HashMap<String, IntegerRangeParameter>();
        var incorrectParameters = new LinkedHashMap<String, Object>();
        var incorrectMaps = new LinkedHashMap<String, Object>();

        input.forEach((key, value) -> {
            if (value instanceof Map) {
                if (NON_NUMERIC_PARAMETERS.containsKey(key)) {
                    incorrectParameters.put(key, value);
                    return;
                }
                if (!((Map<?, ?>) value).keySet().equals(Set.of("range"))) {
                    incorrectMaps.put(key, value);
                    return;
                }
                if (!(((Map<?, ?>) value).get("range") instanceof List)) {
                    incorrectMaps.put(key, value);
                    return;
                }
                var range = (List<?>) ((Map<?, ?>) value).get("range");
                if (range.size() != 2) {
                    incorrectMaps.put(key, value);
                    return;
                }
                var minObject = range.get(0);
                var maxObject = range.get(1);
                if (!typeIsSupportedInRange(minObject) || !typeIsSupportedInRange(maxObject)) {
                    incorrectMaps.put(key, value);
                    return;
                }
                var min = (Number) minObject;
                var max = (Number) maxObject;
                if (isFloatOrDouble(min) || isFloatOrDouble(max)) {
                    var logScale = LOG_SCALE_PARAMETERS.contains(key);
                    doubleRanges.put(key, DoubleRangeParameter.of(min.doubleValue(), max.doubleValue(), logScale));
                    return;
                }
                integerRanges.put(key, IntegerRangeParameter.of(min.intValue(), max.intValue()));
            }
        });
        if (!incorrectParameters.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The following parameters have been given the wrong type: [%s]",
                incorrectParameters
                    .entrySet()
                    .stream()
                    .map(s -> "`" + s + "`" + " (`" + s.getKey() + "` is of type " + NON_NUMERIC_PARAMETERS
                        .get(s.getKey())
                        .getSimpleName() + ")")
                    .collect(Collectors.joining(", "))
            ));
        }
        if (!incorrectMaps.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Ranges for training hyper-parameters must be of the form {range: {min, max}}, " +
                "where both min and max are numerical. Invalid parameters: [%s]",
                incorrectMaps.entrySet().stream().map(s -> "`" + s + "`").collect(Collectors.joining(", "))
            ));
        }
        return ImmutableRangeParameters.of(
            Map.copyOf(doubleRanges),
            Map.copyOf(integerRanges)
        );
    }

    private static boolean typeIsSupportedInRange(Object value) {
        return value instanceof Double || value instanceof Float || value instanceof Integer || value instanceof Long;
    }

    private static boolean isFloatOrDouble(Object value) {
        return value instanceof Double || value instanceof Float;
    }

    static Map<String, ConcreteParameter<?>> parseConcreteParameters(Map<String, Object> input) {
        return input.entrySet().stream()
            .filter(entry -> !(entry.getValue() instanceof Map))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> parseConcreteParameter(entry.getKey(), entry.getValue())
            ));
    }

    private static ConcreteParameter<?> parseConcreteParameter(String key, Object value) {
        if (NON_NUMERIC_PARAMETERS.containsKey(key)) {
            return parseConcreteNonNumericParameter(key, value);
        } else {
            return parseConcreteNumericParameter(key, value);
        }
    }

    private static ConcreteParameter<?> parseConcreteNonNumericParameter(String key, Object value) {
        var correctParameterType = NON_NUMERIC_PARAMETERS.get(key);
        if (!correctParameterType.isInstance(value)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Parameter `%s` must be of the type `%s`.",
                key,
                correctParameterType.getSimpleName()
            ));
        }

        if (correctParameterType == String.class) {
            return StringParameter.of((String) value);
        }

        if (correctParameterType == List.class) {
            if (key.equals("hiddenLayerSizes")) {
                var intValues = ((List<Number>) value).stream().map(Number::intValue).collect(Collectors.toList());
                return ListParameter.of(intValues);
            } else if (key.equals("classWeights")) {
                var doubleValues = ((List<Number>) value).stream().map(Number::doubleValue).collect(Collectors.toList());
                return ListParameter.of(doubleValues);
            }
        }

        throw new IllegalStateException(formatWithLocale(
            "Was not able to resolve type of parameter `%s`.",
            key
        ));
    }

    private static ConcreteParameter<?> parseConcreteNumericParameter(String key, Object value) {
        if (value instanceof Integer) {
            return IntegerParameter.of((Integer) value);
        }
        if (value instanceof Long) {
            return IntegerParameter.of(Math.toIntExact((Long) value));
        }
        if (value instanceof Double) {
            return DoubleParameter.of((Double) value);
        }
        throw new IllegalArgumentException(formatWithLocale(
            "Parameter `%s` must be numeric or a map of the form {range: {min, max}}.",
            key
        ));
    }

    @ValueClass
    interface RangeParameters {
        Map<String, DoubleRangeParameter> doubleRanges();

        Map<String, IntegerRangeParameter> integerRanges();
    }
}
