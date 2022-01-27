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
package org.neo4j.gds.ml.pipeline;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.GdsCallableFinder;
import org.neo4j.gds.executor.ImmutableGdsCallableDefinition;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public final class TestGdsCallableFinder {
    private static final List<String> PACKAGES_TO_SCAN = List.of(
        "org.neo4j.gds"
    );

    public static Optional<GdsCallableFinder.GdsCallableDefinition> findByName(String name) {
        var lowerCaseName = name.toLowerCase(Locale.ROOT);
        return allGdsCallables()
            .filter(callable -> callable.name().toLowerCase(Locale.ROOT).equals(lowerCaseName))
            .findFirst();
    }

    @NotNull
    private static Stream<GdsCallableFinder.GdsCallableDefinition> allGdsCallables() {
        return PACKAGES_TO_SCAN.stream()
            .map(pkg -> new Reflections(pkg, Scanners.TypesAnnotated))
            .flatMap(reflections -> reflections.getTypesAnnotatedWith(GdsCallable.class).stream())
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

    private TestGdsCallableFinder() {}
}
