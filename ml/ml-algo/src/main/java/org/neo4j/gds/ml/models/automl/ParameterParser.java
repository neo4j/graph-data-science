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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.models.automl.TunableTrainerConfig.LOG_SCALE_PARAMETERS;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class ParameterParser {
    private ParameterParser() {}

    static RangeParameters parseRangeParameters(
        Map<String, Object> input
    ) {
        var doubleRanges = new HashMap<String, DoubleRangeParameter>();
        var integerRanges = new HashMap<String, IntegerRangeParameter>();
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
                var minObject = range.get(0);
                var maxObject = range.get(1);
                if (!typeIsSupported(minObject) || !typeIsSupported(maxObject)) {
                    incorrectMaps.add(key);
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
        if (!incorrectMaps.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Ranges for training hyper-parameters must be of the form {range: {min, max}}, " +
                "where both min and max are Float or Integer. Invalid keys: [%s]",
                incorrectMaps.stream().map(s -> "`" + s + "`").collect(Collectors.joining(", "))
            ));
        }
        return ImmutableRangeParameters.of(
            Map.copyOf(doubleRanges),
            Map.copyOf(integerRanges)
        );
    }

    private static boolean typeIsSupported(Object value) {
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

    @ValueClass
    interface RangeParameters {
        Map<String, DoubleRangeParameter> doubleRanges();
        Map<String, IntegerRangeParameter> integerRanges();
    }
}
