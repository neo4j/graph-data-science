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
import org.neo4j.gds.ml.models.automl.hyperparameter.DoubleRangeParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.IntegerRangeParameter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.models.automl.ParameterParser.parseConcreteParameters;
import static org.neo4j.gds.ml.models.automl.ParameterParser.parseDoubleRanges;
import static org.neo4j.gds.ml.models.automl.ParameterParser.parseIntegerRanges;
import static org.neo4j.gds.ml.models.automl.ParameterParser.validateSyntax;

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
        validateSyntax(userInput);
        var defaults = method.createConfig(Map.of()).toMap();
        var inputWithDefaults = fillDefaults(userInput, defaults);
        var concreteParameters = parseConcreteParameters(inputWithDefaults);
        var doubleRanges = parseDoubleRanges(userInput);
        var integerRanges = parseIntegerRanges(userInput);
        var tunableTrainerConfig = new TunableTrainerConfig(concreteParameters, doubleRanges, integerRanges, method);
        // triggers validation for combinations of end endpoints of each range.
        tunableTrainerConfig.materializeConcreteCube();
        return tunableTrainerConfig;
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

    public TrainerConfig materialize(Map<String, Object> hyperParameterValues) {
        var materializedMap = new HashMap<String, Object>();
        concreteParameters.forEach((key, value) -> materializedMap.put(key, value.value()));
        materializedMap.putAll(hyperParameterValues);
        return trainingMethod().createConfig(materializedMap);
    }

    public List<TrainerConfig> materializeConcreteCube() {
        var result = new ArrayList<TrainerConfig>();
        var numberOfHyperParameters = doubleRanges.size() + integerRanges.size();
        var doubleRangeKeys = new ArrayList<>(doubleRanges.keySet());
        var integerRangeKeys = new ArrayList<>(integerRanges.keySet());
        if (numberOfHyperParameters > 32)
            throw new IllegalArgumentException("Currently at most 32 hyperparameters are supported");
        for (int bitset = 0; bitset < Math.pow(2, numberOfHyperParameters); bitset++) {
            var hyperParameterValues = new HashMap<String, Object>();
            for (int parameterIdx = 0; parameterIdx < numberOfHyperParameters; parameterIdx++) {
                boolean useMin = (bitset >> parameterIdx & 1) == 0;
                if (parameterIdx < doubleRanges.size()) {
                    var key = doubleRangeKeys.get(parameterIdx);
                    var doubleRange = doubleRanges.get(key);
                    var materializedValue = useMin ? doubleRange.min() : doubleRange.max();
                    hyperParameterValues.put(key, materializedValue);
                } else {
                    var key = integerRangeKeys.get(parameterIdx - doubleRanges.size());
                    var intRange = integerRanges.get(key);
                    var materializedValue = useMin ? intRange.min() : intRange.max();
                    hyperParameterValues.put(key, materializedValue);
                }
            }
            result.add(materialize(hyperParameterValues));
        }
        return result;
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
