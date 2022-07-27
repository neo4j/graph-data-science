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
package org.neo4j.gds.executor;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.AlgoBaseConfig;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public final class GdsCallableFinder {

    private static final List<String> DEFAULT_PACKAGE_BLACKLIST = List.of("org.neo4j.gds.pregel");

    public static Stream<GdsCallableDefinition> findAll() {
        return findAll(DEFAULT_PACKAGE_BLACKLIST);
    }

    public static Stream<GdsCallableDefinition> findAll(Collection<String> blacklist) {
        return allGdsCallables(blacklist).sorted(Comparator.comparing(
            GdsCallableDefinition::name,
            String.CASE_INSENSITIVE_ORDER
        ));
    }

    public static Optional<GdsCallableDefinition> findByName(String name) {
        return findByName(name, DEFAULT_PACKAGE_BLACKLIST);
    }

    public static Optional<GdsCallableDefinition> findByName(String name, Collection<String> blacklist) {
        var lowerCaseName = name.toLowerCase(Locale.ROOT);
        return allGdsCallables(blacklist)
            .filter(callable -> callable.name().toLowerCase(Locale.ROOT).equals(lowerCaseName))
            .findFirst();
    }

    @NotNull
    private static Stream<GdsCallableDefinition> allGdsCallables(Collection<String> blacklist) {
        return ClassesHolder.CALLABLE_CLASSES.stream()
            .filter(clazz -> blacklist
                .stream()
                .noneMatch(item -> clazz.getPackageName().startsWith(item)))
            .map(clazz -> {
                GdsCallable gdsCallable = clazz.getAnnotation(GdsCallable.class);
                return ImmutableGdsCallableDefinition
                    .builder()
                    .name(gdsCallable.name())
                    .description(gdsCallable.description())
                    .executionMode(gdsCallable.executionMode())
                    .algorithmSpecClass((Class<AlgorithmSpec<Algorithm<Object>, Object, AlgoBaseConfig, Object, AlgorithmFactory<?, Algorithm<Object>, AlgoBaseConfig>>>) clazz)
                    .build();
            });
    }

    private static final class ClassesHolder {
        private static final List<Class<?>> CALLABLE_CLASSES = loadPossibleClasses();

        @NotNull
        private static List<Class<?>> loadPossibleClasses() {
            var pathFromJar = "META-INF/services/" + GdsCallable.class.getCanonicalName();
            var pathFromResourcesFolder = "/" + pathFromJar;

            var classes = new ArrayList<Class<?>>();
            classes.addAll(loadPossibleClassesFrom(pathFromJar));
            classes.addAll(loadPossibleClassesFrom(pathFromResourcesFolder));

            return classes;
        }

        @NotNull
        private static List<Class<?>> loadPossibleClassesFrom(String path) {
            var classLoader = Objects.requireNonNullElse(
                Thread.currentThread().getContextClassLoader(),
                ClassesHolder.class.getClassLoader()
            );

            try (var callablesStream = classLoader.getResourceAsStream(path)) {
                if (callablesStream == null) {
                    return List.of();
                }
                try (var callables = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(callablesStream),
                    StandardCharsets.UTF_8
                ))) {
                    return callables.lines()
                        .map(clazz -> {
                            try {
                                return classLoader.loadClass(clazz);
                            } catch (ClassNotFoundException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .peek(clazz -> {assert AlgorithmSpec.class.isAssignableFrom(clazz);})
                        .collect(Collectors.toList());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @ValueClass
    public interface GdsCallableDefinition {
        Class<AlgorithmSpec<Algorithm<Object>, Object, AlgoBaseConfig, Object, AlgorithmFactory<?, Algorithm<Object>, AlgoBaseConfig>>> algorithmSpecClass();

        @Value.Lazy
        default AlgorithmSpec<Algorithm<Object>, Object, AlgoBaseConfig, Object, AlgorithmFactory<?, Algorithm<Object>, AlgoBaseConfig>> algorithmSpec() {
            try {
                return algorithmSpecClass().getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        String name();

        String description();

        ExecutionMode executionMode();
    }

    private GdsCallableFinder() {}
}
