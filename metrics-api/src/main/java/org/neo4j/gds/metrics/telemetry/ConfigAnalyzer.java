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
package org.neo4j.gds.metrics.telemetry;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class ConfigAnalyzer {

    public static List<String> nonDefaultParameters(AlgoBaseConfig config) {
        Class<?>[] interfaces = config.getClass().getInterfaces();

        if (interfaces.length != 1) {
            throw new IllegalArgumentException("Config class must implement exactly one interface");
        }

        Class<?> configInterface = interfaces[0];

        return Arrays.stream(interfaces[0].getMethods())
            .filter(method -> method.getAnnotation(Configuration.Ignore.class) == null)
            .filter(method -> method.getAnnotation(Configuration.CollectKeys.class) == null)
            .filter(method -> method.getAnnotation(Configuration.ToMap.class) == null)
            .filter(method -> method.getParameters().length == 0)
            .filter(method ->
                optionalValueIsSet(config, method) || valueEqualsDefaultImplementation(config, configInterface, method))
            .map(Method::getName)
            .filter(name -> !name.equals("jobId"))
            .toList();
    }

    private static boolean optionalValueIsSet(AlgoBaseConfig config, Method method) {
        if (method.getReturnType().equals(Optional.class)) {
            try {
                return !method.invoke(config).equals(Optional.empty());
            } catch (IllegalAccessException | InvocationTargetException e) {
                return false;
            }
        }
        return false;
    }

    private static boolean valueEqualsDefaultImplementation(
        AlgoBaseConfig config,
        Class<?> configInterface,
        Method method
    ) {
        if (!method.isDefault()) {
            return false;
        }

        try {
            var defaultValue = MethodHandles.lookup().unreflectSpecial(method, configInterface)
                .bindTo(config)
                .invoke();

            var configureValue = config.getClass().getMethod(method.getName()).invoke(config);

            return !Objects.equals(defaultValue, configureValue);

        } catch (Throwable e) {
            return false;
        }
    }

    private ConfigAnalyzer() {}
}
