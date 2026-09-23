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
package org.neo4j.gds.pyruntime.graphsage;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

// mirrors data_science_python_runtime.algorithms.graphsage.gs_base.ActivationFunction
public enum GraphSageActivationFunction {
    RELU("relu"), SIGMOID("sigmoid");

    private final String value;

    GraphSageActivationFunction(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static GraphSageActivationFunction parse(Object input) {
        if (input instanceof GraphSageActivationFunction activationFunction) {
            return activationFunction;
        }
        if (input instanceof String string) {
            return Arrays.stream(values())
                .filter(a -> a.value.equalsIgnoreCase(string) || a.name().equalsIgnoreCase(string))
                .findFirst()
                .orElseThrow(
                    () -> new IllegalArgumentException(
                        String.format(
                            Locale.ENGLISH,
                            "ActivationFunction `%s` is not supported. Must be one of: %s.",
                            input,
                            supportedValues()
                        )
                    )
                );
        }
        throw new IllegalArgumentException(
            String.format(
                Locale.ENGLISH,
                "Expected ActivationFunction or String. Got %s.",
                input.getClass().getSimpleName()
            )
        );
    }

    public static String toMapValue(GraphSageActivationFunction activationFunction) {
        return activationFunction.value;
    }

    private static String supportedValues() {
        return Arrays.stream(values()).map(a -> a.value).collect(Collectors.joining(", "));
    }
}
