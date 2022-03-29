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

import org.neo4j.gds.ml.models.TrainingMethod;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TunableTrainerConfig {
    public final Map<String, Object> value;
    private final TrainingMethod method;

    private TunableTrainerConfig(Map<String, Object> value, TrainingMethod method) {
        this.value = value;
        this.method = method;
    }

    public static TunableTrainerConfig of(Map<String, Object> value, TrainingMethod method) {
        return new TunableTrainerConfig(fillDefaults(value, method), method);
    }

    private static Map<String, Object> fillDefaults(Map<String, Object> value, TrainingMethod method) {
        var defaultConfig = method.createConfig(Map.of()).toMap();
        // for values that have type Optional<?>, defaultConfig will not contain the key so we need keys from both maps
        // if such keys are missing from the `value` map, then we also do not want to add them
        return Stream.concat(defaultConfig.keySet().stream(), value.keySet().stream())
            .distinct()
            .collect(Collectors.toMap(
                key -> key,
                key -> value.getOrDefault(key, defaultConfig.getOrDefault(key, Optional.empty()))
            ));
    }

    public Map<String, Object> toMap() {
        return value.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                if (entry.getValue() instanceof Long) {
                    return Math.toIntExact((Long) entry.getValue());
                }
                return entry.getValue();
            }));
    }

    public String methodName() {
        return method.name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TunableTrainerConfig that = (TunableTrainerConfig) o;
        return Objects.equals(value, that.value) &&
               method == that.method;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, method);
    }
}
