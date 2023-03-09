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
import org.neo4j.gds.ml.models.automl.ParameterParser.RangeParameters;
import org.neo4j.gds.ml.models.automl.hyperparameter.ConcreteParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.DoubleRangeParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.IntegerRangeParameter;
import org.neo4j.gds.ml.models.automl.hyperparameter.NumericalRangeParameter;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.mlp.MLPClassifierTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressorTrainerConfig;
import org.neo4j.gds.ml_api.TrainingMethod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.models.automl.ParameterParser.parseConcreteParameters;
import static org.neo4j.gds.ml.models.automl.ParameterParser.parseRangeParameters;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TunableTrainerConfig {
    static final double EPSILON = 1e-8;
    static final List<String> LOG_SCALE_PARAMETERS = List.of("penalty", "learningRate", "tolerance");
    // If you add a new entry here, make sure that `ParameterParser.parseConcreteNonNumericParameter` can handle the value type.
    static final Map<String, Class> NON_NUMERIC_PARAMETERS = Map.of("criterion", String.class, "hiddenLayerSizes", List.class, "classWeights", List.class);
    private final Map<String, ConcreteParameter<?>> concreteParameters;
    public final Map<String, DoubleRangeParameter> doubleRanges;
    public final Map<String, IntegerRangeParameter> integerRanges;
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
        RangeParameters rangeParameters = parseRangeParameters(userInput);
        var defaults = createTrainerConfigFromMap(Map.of(), method).toMap();
        var inputWithDefaults = fillDefaults(userInput, defaults);
        var concreteParameters = parseConcreteParameters(inputWithDefaults);
        var tunableTrainerConfig = new TunableTrainerConfig(
            concreteParameters,
            rangeParameters.doubleRanges(),
            rangeParameters.integerRanges(),
            method
        );
        // triggers validation for combinations of end endpoints of each range.
        tunableTrainerConfig.streamCornerCaseConfigs().forEach(unused -> {});
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
        return createTrainerConfigFromMap(materializedMap, method);
    }

    public Stream<TrainerConfig> streamCornerCaseConfigs() {
        var rangeParameters = new HashMap<String, NumericalRangeParameter<?>>();

        rangeParameters.putAll(doubleRanges);
        rangeParameters.putAll(integerRanges);

        var numberOfHyperParameters = rangeParameters.size();
        if (numberOfHyperParameters > 20)
            throw new IllegalArgumentException("Currently at most 20 hyperparameters are supported");

        // the position i in the bitset represents whether to take min or max value for the i:th parameter
        return IntStream.range(0, (int) Math.pow(2, numberOfHyperParameters)).mapToObj(bitset -> {
            var hyperParameterValues = new HashMap<String, Object>();
            var parameterIdx = 0;
            for (var entry : rangeParameters.entrySet()) {
                boolean useMin = (bitset >> parameterIdx & 1) == 0;
                var range = entry.getValue();
                var key = entry.getKey();
                var materializedValue = endPoint(useMin, doubleRanges.containsKey(key), range);
                hyperParameterValues.put(key, materializedValue);
                parameterIdx++;
            }
            return materialize(hyperParameterValues);
        });
    }

    private static Number endPoint(boolean useMin, boolean isDouble, NumericalRangeParameter<?> range) {
        // some parameters have exclusive endpoints, for example RandomForestTrainConfig.maxFeaturesRatio.
        // this prevents us from materializing at the endpoint
        if (isDouble) {
            return useMin ? (Double) range.min() + EPSILON : (Double) range.max() - EPSILON;
        } else {
            return useMin ? range.min() : range.max();
        }
    }

    public Map<String, Object> toMap() {
        var result = new HashMap<String, Object>();
        concreteParameters.forEach((key, value) -> result.put(key, value.value()));
        doubleRanges.forEach((key, value) -> result.put(key, value.toMap()));
        integerRanges.forEach((key, value) -> result.put(key, value.toMap()));
        // this is an ignored field that should still be part of the toMap version
        result.put("methodName", trainingMethod().toString());
        return result;
    }

    public TrainingMethod trainingMethod() {
        return method;
    }

    public boolean isConcrete() {
        return doubleRanges.isEmpty() && integerRanges.isEmpty();
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

    private static TrainerConfig createTrainerConfigFromMap(Map<String, Object> configMap, TrainingMethod method) {
        switch (method)  {
            case LogisticRegression: return LogisticRegressionTrainConfig.of(configMap);
            case RandomForestClassification: return RandomForestClassifierTrainerConfig.of(configMap);
            case MLPClassification: return MLPClassifierTrainConfig.of(configMap);
            case LinearRegression: return LinearRegressionTrainConfig.of(configMap);
            case RandomForestRegression: return RandomForestRegressorTrainerConfig.of(configMap);
            default: throw new IllegalStateException(formatWithLocale("Method %s does not have a trainerConfig Implemented", method.name()));
        }
    }
}
