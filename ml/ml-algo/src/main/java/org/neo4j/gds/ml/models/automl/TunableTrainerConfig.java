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
import org.neo4j.gds.ml.models.automl.hyperparameter.HyperParameterValues;
import org.neo4j.gds.ml.models.automl.hyperparameter.IntegerParameter;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TunableTrainerConfig {
    private final Map<String, ConcreteParameter<?>> concreteValues;
    private final TrainingMethod method;

    private TunableTrainerConfig(Map<String, ConcreteParameter<?>> concreteValues, TrainingMethod method) {
        this.concreteValues = concreteValues;
        this.method = method;
    }

    public static TunableTrainerConfig of(Map<String, Object> userInput, TrainingMethod method) {
        var defaults = method.createConfig(Map.of()).toMap();
        var inputWithDefaults = fillDefaults(userInput, defaults);
        var parsedParameters = parse(inputWithDefaults);
        return new TunableTrainerConfig(parsedParameters, method);
    }

    private static Map<String, ConcreteParameter<?>> parse(Map<String, Object> input) {
        return input.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    var val = entry.getValue();
                    return parseParameterValue(entry.getKey(), val);
                }));
    }

    private static ConcreteParameter<?> parseParameterValue(String key, Object value) {
        if (value instanceof Integer) {
            return IntegerParameter.of((Integer) value);
        }
        if (value instanceof Long) {
            return IntegerParameter.of(Math.toIntExact((Long) value));
        }
        if (value instanceof Double) {
            return DoubleParameter.of((Double) value);
        }
        throw new IllegalArgumentException(formatWithLocale("Parameter `%s` must be numeric", key));
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
        var materializedMap = concreteValues.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> (Object) entry.getValue().value()
            ));
        return trainingMethod().createConfig(materializedMap);
    }

    public Map<String, Object> toMap() {
        var result = concreteValues.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> (Object) entry.getValue().value())
            );
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
        return Objects.equals(concreteValues, that.concreteValues) &&
               method == that.method;
    }

    @Override
    public int hashCode() {
        return Objects.hash(concreteValues, method);
    }
}
