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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
                if (range.get(0) instanceof Double && range.get(1) instanceof Double) {
                    doubleRanges.put(key, DoubleRangeParameter.of((Double) range.get(0), (Double) range.get(1)));
                    return;
                }
                if ((range.get(0) instanceof Integer && range.get(1) instanceof Integer) ||
                    (range.get(0) instanceof Long && range.get(1) instanceof Long)) {
                    integerRanges.put(key, IntegerRangeParameter.of(toInt(range.get(0)), toInt(range.get(1))));
                    return;
                }
                incorrectMaps.add(key);
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
            Collections.unmodifiableMap(doubleRanges),
            Collections.unmodifiableMap(integerRanges)
        );
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

    private static int toInt(Object wholeNumber) {
        if (wholeNumber instanceof Integer) return (int) wholeNumber;
        return Math.toIntExact((Long) wholeNumber);
    }

    @ValueClass
    interface RangeParameters {
        Map<String, DoubleRangeParameter> doubleRanges();
        Map<String, IntegerRangeParameter> integerRanges();
    }
}
