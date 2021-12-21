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
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public final class GdsCallableFinder {
    private static final List<String> PACKAGES_TO_SCAN = List.of(
        "org.neo4j.gds"
    );

    private static final List<String> DEFAULT_PACKAGE_BLACKLIST = List.of(
        "org.neo4j.gds.pregel",
        "org.neo4j.gds.test"
    );

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
        return PACKAGES_TO_SCAN.stream()
            .map(pkg -> new Reflections(pkg, Scanners.TypesAnnotated))
            .flatMap(reflections -> reflections.getTypesAnnotatedWith(GdsCallable.class).stream())
            .filter(clazz -> blacklist
                .stream()
                .noneMatch(item -> clazz.getPackageName().startsWith(item)))
            .peek(clazz -> {assert AlgorithmSpec.class.isAssignableFrom(clazz);})
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

    @ValueClass
    public interface GdsCallableDefinition {
        Class<AlgorithmSpec<Algorithm<Object>, Object, AlgoBaseConfig, Object, AlgorithmFactory<?, Algorithm<Object>, AlgoBaseConfig>>> algorithmSpecClass();

        @Value.Lazy
        default AlgorithmSpec<Algorithm<Object>, Object, AlgoBaseConfig, Object, AlgorithmFactory<?, Algorithm<Object>, AlgoBaseConfig>> algorithmSpec() {
            try {
                return algorithmSpecClass().getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        String name();

        String description();

        ExecutionMode executionMode();
    }

    private GdsCallableFinder() {}
}
