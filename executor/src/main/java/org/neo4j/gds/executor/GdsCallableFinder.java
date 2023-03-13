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
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class GdsCallableFinder {

    private static final List<String> DEFAULT_PACKAGE_DENY_LIST = List.of();

    public static Stream<GdsCallableDefinition> findAll() {
        return findAll(DEFAULT_PACKAGE_DENY_LIST);
    }

    public static Stream<GdsCallableDefinition> findAll(Collection<String> denyList) {
        return allGdsCallables(denyList).sorted(Comparator.comparing(
            GdsCallableDefinition::name,
            String.CASE_INSENSITIVE_ORDER
        ));
    }

    public static Optional<GdsCallableDefinition> findByName(String name) {
        return findByName(name, DEFAULT_PACKAGE_DENY_LIST);
    }

    public static Optional<GdsCallableDefinition> findByName(String name, Collection<String> denyList) {
        return Optional
            .ofNullable(ClassesHolder.CALLABLE_CLASSES.get(name.toLowerCase(Locale.ENGLISH)))
            .filter(block(denyList));
    }

    @NotNull
    private static Stream<GdsCallableDefinition> allGdsCallables(Collection<String> denyList) {
        return ClassesHolder.CALLABLE_CLASSES.values().stream().filter(block(denyList));
    }

    private static Predicate<GdsCallableDefinition> block(Collection<String> denyList) {
        return def -> denyList
            .stream()
            .noneMatch(item -> def.algorithmSpecClass().getPackageName().startsWith(item));
    }

    private static final class ClassesHolder {
        private static final Map<String, GdsCallableDefinition> CALLABLE_CLASSES = loadPossibleClasses();

        @NotNull
        private static Map<String, GdsCallableDefinition> loadPossibleClasses() {
            var classLoader = Objects.requireNonNullElse(
                Thread.currentThread().getContextClassLoader(),
                ClassesHolder.class.getClassLoader()
            );

            var classes = new ArrayList<Class<?>>();

            // This is a bit of a hack, for the actual estimation-cli, wo don't want to scan the classpath.
            // but for tests, we don't have all the values collected in the services files
            var collectViaScanning = StackWalker
                .getInstance()
                .walk(s -> s.reduce((l, r) -> r)) // get the last stack frame, our main method
                .map(StackWalker.StackFrame::getClassName)
                .filter(c -> c.equals("com.neo4j.gds.estimation.cli.EstimationCli"))
                .isEmpty();

            // This is a bit of a hack, for some reason `Reflections` can't find any `GdsCallable`s when running against real Neo4j 5.x,
            // we believe it is due to JDK 17 + MR Jar packaging but have no concrete proof
            // If we need to collect callables via scanning try to do it;
            // if no callables were found `classes.addAll` returns `false` => fallback to loading from META-INF
            boolean didCollectCallablesViaScanning = collectViaScanning && classes.addAll(loadPossibleClassesViaClasspathScanning(classLoader));

            if (!didCollectCallablesViaScanning) {
                classes.addAll(loadPossibleClassesFromJar(classLoader));
                classes.addAll(loadPossibleClassesFromResourcesFolder(classLoader));
            }

            assert assertAllAreAlgoSpec(classes);

            return classes
                .stream()
                .flatMap(clazz -> {
                    GdsCallables callables = clazz.getAnnotation(GdsCallables.class);
                    if (callables == null) {
                        GdsCallable callable = clazz.getAnnotation(GdsCallable.class);
                        return Stream.of(ImmutableClassAndCallable.of(clazz, callable));
                    }
                    return Arrays.stream(callables.value()).map(gdsCallable -> ImmutableClassAndCallable.of(clazz, gdsCallable));
                }).map(clazzAndCallable -> {
                    var gdsCallable = clazzAndCallable.gdsCallable();
                    var clazz = clazzAndCallable.clazz();
                    //noinspection unchecked
                    return ImmutableGdsCallableDefinition
                        .builder()
                        .name(gdsCallable.name())
                        .description(gdsCallable.description())
                        .executionMode(gdsCallable.executionMode())
                        .algorithmSpecClass((Class<AlgorithmSpec<Algorithm<Object>, Object, AlgoBaseConfig, Object, AlgorithmFactory<?, Algorithm<Object>, AlgoBaseConfig>>>) clazz)
                        .build();
                })
                .collect(Collectors.toMap(
                    def -> def.name().toLowerCase(Locale.ENGLISH),
                    Function.identity(),
                    (l, r) -> l
                ));
        }

        private static List<Class<?>> loadPossibleClassesFromJar(ClassLoader classLoader) {
            var singularlyAnnotated = loadPossibleClassesFrom(
                classLoader,
                "META-INF/services/" + GdsCallable.class.getCanonicalName()
            );
            var plurallyAnnotated = loadPossibleClassesFrom(
                classLoader,
                "META-INF/services/" + GdsCallables.class.getCanonicalName()
            );
            var result = new ArrayList<Class<?>>();
            result.addAll(singularlyAnnotated);
            result.addAll(plurallyAnnotated);
            return result;
        }

        private static List<Class<?>> loadPossibleClassesFromResourcesFolder(ClassLoader classLoader) {
            var singularlyAnnotated = loadPossibleClassesFrom(
                classLoader,
                "/META-INF/services/" + GdsCallable.class.getCanonicalName()
            );
            var plurallyAnnotated = loadPossibleClassesFrom(
                classLoader,
                "/META-INF/services/" + GdsCallables.class.getCanonicalName()
            );
            var result = new ArrayList<Class<?>>();
            result.addAll(singularlyAnnotated);
            result.addAll(plurallyAnnotated);
            return result;
        }

        private static List<Class<?>> loadPossibleClassesFrom(ClassLoader classLoader, String path) {
            var callablesStream = classLoader.getResourceAsStream(path);
            if (callablesStream == null) {
                return List.of();
            }

            try (callablesStream) {
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
                        .collect(Collectors.toList());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static List<Class<?>> loadPossibleClassesViaClasspathScanning(ClassLoader classLoader) {
            return Stream.of("org.neo4j.gds")
                .map(pkg -> new Reflections(new ConfigurationBuilder()
                    .addClassLoaders(classLoader)
                    .forPackage(pkg, classLoader)
                    .addScanners(Scanners.TypesAnnotated)
                    .filterInputsBy(new FilterBuilder().includePackage(pkg))))
                .flatMap(reflections -> Stream.concat(
                    reflections.getTypesAnnotatedWith(GdsCallable.class).stream(),
                    reflections.getTypesAnnotatedWith(GdsCallables.class).stream()
                ))
                .collect(Collectors.toList());
        }

        private static boolean assertAllAreAlgoSpec(Iterable<Class<?>> classes) {
            for (var clazz : classes) {
                assert AlgorithmSpec.class.isAssignableFrom(clazz);
            }
            return true;
        }
    }

    @ValueClass
    interface ClassAndCallable {
        Class<?> clazz();
        GdsCallable gdsCallable();
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
