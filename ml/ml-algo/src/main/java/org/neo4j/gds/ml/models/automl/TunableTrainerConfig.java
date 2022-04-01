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

import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.hyperparameter.ConcreteParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.DoubleParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.DoubleRangeParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.HyperParameterValues;
import org.neo4j.gds.ml.models.automl.hyperparameter.IntegerParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.IntegerRangeParameter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TunableTrainerConfig {
    private final Map<String, ConcreteParameter<?>> concreteParameters;
    private final Map<String, DoubleRangeParameter> doubleRanges;
    private final Map<String, IntegerRangeParameter> integerRanges;
    private final TrainingMethod method;

    private TunableTrainerConfig(
        Map<String, ConcreteParameter<?>> concreteParameters,
        Map<String, DoubleRangeParameter> doubleRanges,
        Map<String, IntegerRangeParameter> integerRanges,
        TrainingMethod method
    ) {
        this.concreteParameters = concreteParameters;
        this.doubleRanges = doubleRanges;
        this.integerRanges = integerRanges;
        this.method = method;
    }

    public static TunableTrainerConfig of(Map<String, Object> userInput, TrainingMethod method) {
        validateMaps(userInput);
        var defaults = method.createConfig(Map.of()).toMap();
        var inputWithDefaults = fillDefaults(userInput, defaults);
        var concreteParameters = parseConcreteParameters(inputWithDefaults);
        var doubleRanges = parseDoubleRanges(userInput);
        var integerRanges = parseIntegerRanges(userInput);
        return new TunableTrainerConfig(concreteParameters, doubleRanges, integerRanges, method);
    }

    private static void validateMaps(Map<String, Object> input) {
        var incorrectMaps = new ArrayList<String>();

        input.forEach((key, value) -> {
            if (value instanceof Map) {
                if (!((Map<?, ?>) value).keySet().equals(Set.of("range"))) {
                    incorrectMaps.add(key);
                    return;
                }
                if (!(((Map<?, ?>) value).get("range") instanceof List)) {
                    incorrectMaps.add(key);
                    return;
                }
                var range = (List<?>) ((Map<?, ?>) value).get("range");
                if (range.size() != 2) {
                    incorrectMaps.add(key);
                    return;
                }
                if (range.get(0) instanceof Double && range.get(1) instanceof Double) return;
                if (range.get(0) instanceof Integer && range.get(1) instanceof Integer) return;
                if (range.get(0) instanceof Long && range.get(1) instanceof Long) return;
                incorrectMaps.add(key);
            }
        });
        if (!incorrectMaps.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Map parameters must be of the form {range: {min, max}}, " +
                "where both min and max are Float or Integer. Invalid keys: [%s]",
                incorrectMaps.stream().map(s -> "`" + s + "`").collect(Collectors.joining(", "))
            ));
        }
    }

    private static Map<String, DoubleRangeParameter> parseDoubleRanges(Map<String, Object> input) {
        var result = new HashMap<String, DoubleRangeParameter>();

        input.forEach((key, value) -> {
            if (!(value instanceof Map)) return;
            var range = (List<?>) ((Map<?, ?>) value).get("range");
            if (range.get(0) instanceof Double && range.get(1) instanceof Double) {
                result.put(key, DoubleRangeParameter.of((Double) range.get(0), (Double) range.get(1)));
            }
        });
        return result;
    }

    private static Map<String, IntegerRangeParameter> parseIntegerRanges(Map<String, Object> input) {
        var result = new HashMap<String, IntegerRangeParameter>();

        input.forEach((key, value) -> {
            if (!(value instanceof Map)) return;
            var range = (List<?>) ((Map<?, ?>) value).get("range");
            if (range.get(0) instanceof Integer && range.get(1) instanceof Integer) {
                result.put(key, IntegerRangeParameter.of((Integer) range.get(0), (Integer) range.get(1)));
            }
            if (range.get(0) instanceof Long && range.get(1) instanceof Long) {
                result.put(key,
                    IntegerRangeParameter.of(
                        Math.toIntExact((Long) range.get(0)),
                        Math.toIntExact((Long) range.get(1))
                    )
                );
            }
        });
        return result;
    }

    private static Map<String, ConcreteParameter<?>> parseConcreteParameters(Map<String, Object> input) {
        var result = new HashMap<String, ConcreteParameter<?>>();
        input.forEach((key, value) -> {
                if (value instanceof Map) return;
                result.put(key, parseConcreteParameter(key, value));
            }
        );
        return result;
    }

    private static ConcreteParameter<?> parseConcreteParameter(String key, Object value) {
        if (value instanceof Integer) {
            return IntegerParameter.of((Integer) value);
        }
        if (value instanceof Long) {
            return IntegerParameter.of(Math.toIntExact((Long) value));
        }
        if (value instanceof Double) {
            return DoubleParameter.of((Double) value);
        }
        throw new IllegalArgumentException(formatWithLocale("Parameter `%s` must be numeric or of the form {range: {min, max}}.", key));
    }

    private static Map<String, Object> fillDefaults(
        Map<String, Object> userInput,
        Map<String, Object> defaults
    ){
        // for values that have type Optional<?>, defaults will not contain the key so we need keys from both maps
        // if such keys are missing from the `value` map, then we also do not want to add them
        return Stream.concat(defaults.keySet().stream(), userInput.keySet().stream())
            .distinct()
            .filter(key -> !key.equals("methodName"))
            .collect(Collectors.toMap(
                key -> key,
                key -> userInput.getOrDefault(key, defaults.get(key))
            ));
    }

    public TrainerConfig materialize(HyperParameterValues hyperParameterValues) {
        var materializedMap = new HashMap<String, Object>();
        concreteParameters.forEach((key, value) -> materializedMap.put(key, value.value()));
        materializedMap.putAll(hyperParameterValues.values);
        return trainingMethod().createConfig(materializedMap);
    }

    public Map<String, Object> toMap() {
        var result = new HashMap<String, Object>();
        concreteParameters.forEach((key, value) -> result.put(key, value.value()));
        doubleRanges.forEach((key, value) -> result.put(key, value.toMap()));
        integerRanges.forEach((key, value) -> result.put(key, value.toMap()));
        result.put("methodName", trainingMethod().name());
        return result;
    }

    public TrainingMethod trainingMethod() {
        return method;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TunableTrainerConfig that = (TunableTrainerConfig) o;
        return Objects.equals(concreteParameters, that.concreteParameters) &&
               method == that.method;
    }

    @Override
    public int hashCode() {
        return Objects.hash(concreteParameters, method);
    }
}
