/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.extension;

import org.immutables.value.Value;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.gdl.GdlFactory;
import org.neo4j.graphalgo.gdl.ImmutableGraphCreateFromGdlConfig;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;
import static org.mockito.internal.util.reflection.FieldSetter.setField;

public class GdlSupportExtension implements BeforeEachCallback, AfterEachCallback {

    @Override
    public void beforeEach(ExtensionContext context) {
        Class<?> requiredTestClass = context.getRequiredTestClass();
        gdlGraphs(requiredTestClass).forEach(setup -> injectGraphStore(setup, context));
    }

    @Override
    public void afterEach(ExtensionContext context) {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    private static Collection<GdlGraphSetup> gdlGraphs(Class<?> testClass) {
        var setups = new HashSet<GdlGraphSetup>();

        do {
            stream(testClass.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(GdlGraph.class) || f.isAnnotationPresent(GdlGraphs.class))
                .flatMap(GdlSupportExtension::gdlGraphsForField)
                .peek(setup -> {
                    if (setups.contains(setup)) {
                        throw new ExtensionConfigurationException(String.format(
                            Locale.ENGLISH,
                            "Graph names must be unique. Found duplicate graph name '%s'.",
                            setup.graphName()
                        ));
                    }
                })
                .forEach(setups::add);

            testClass = testClass.getSuperclass();
        } while (testClass != null);

        if (setups.isEmpty()) {
            throw new ExtensionConfigurationException(String.format(
                Locale.ENGLISH,
                "At least one field must be annotated with %s.",
                GdlGraph.class.getTypeName()
            ));
        }

        return setups;
    }

    private static Stream<GdlGraphSetup> gdlGraphsForField(Field field) {
        if (field.getType() != String.class) {
            throw new ExtensionConfigurationException(String.format(
                Locale.ENGLISH,
                "Field %s.%s must be of type %s.",
                field.getDeclaringClass().getTypeName(),
                field.getName(),
                String.class.getTypeName()
            ));
        }

        // read field value
        if (!Modifier.isStatic(field.getModifiers())) {
            throw new ExtensionConfigurationException(String.format(
                Locale.ENGLISH,
                "Field %s.%s must be static.",
                field.getDeclaringClass().getTypeName(),
                field.getName()
            ));
        }

        if (Modifier.isPrivate(field.getModifiers())) {
            field.setAccessible(true);
        }

        String gdl;
        try {
            gdl = field.get(null).toString();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        var annotations = field.isAnnotationPresent(GdlGraph.class)
            ? Stream.of(field.getAnnotation(GdlGraph.class))
            : Arrays.stream(field.getAnnotation(GdlGraphs.class).value());

        return annotations
            .map(annotation -> ImmutableGdlGraphSetup.of(
                annotation.graphName(),
                gdl,
                annotation.username(),
                annotation.orientation(),
                annotation.addToCatalog()
            ));
    }

    private static void injectGraphStore(GdlGraphSetup gdlGraphSetup, ExtensionContext context) {
        String graphName = gdlGraphSetup.graphName();

        var createConfig = ImmutableGraphCreateFromGdlConfig.builder()
            .username(gdlGraphSetup.username())
            .graphName(graphName)
            .gdlGraph(gdlGraphSetup.gdlGraph())
            .orientation(gdlGraphSetup.orientation())
            .build();

        GdlFactory gdlFactory = GdlFactory.of(createConfig);
        GraphStore graphStore = gdlFactory.build().graphStore();
        Graph graph = graphStore.getUnion();
        IdFunction idFunction = gdlFactory::nodeId;

        if (gdlGraphSetup.addToCatalog()) {
            GraphStoreCatalog.set(createConfig, graphStore);
        }

        context.getRequiredTestInstances().getAllInstances().forEach(testInstance -> {
            injectInstance(testInstance, graphName, graph, Graph.class);
            injectInstance(testInstance, graphName, graphStore, GraphStore.class);
            injectInstance(testInstance, graphName, idFunction, IdFunction.class);
        });
    }

    private static <T> void injectInstance(Object testInstance, String graphName, T instance, Class<T> clazz) {
        Class<?> testClass = testInstance.getClass();
        do {
            stream(testClass.getDeclaredFields())
                .filter(field -> field.getType() == clazz)
                .filter(field -> isAnnotated(field, Inject.class))
                .filter(field -> field.getAnnotation(Inject.class).graphName().equals(graphName))
                .forEach(field -> setField(testInstance, field, instance));
            testClass = testClass.getSuperclass();
        }
        while (testClass != null);
    }

    @ValueClass
    interface GdlGraphSetup {
        String graphName();

        @Value.Auxiliary
        String gdlGraph();

        @Value.Auxiliary
        String username();

        @Value.Auxiliary
        Orientation orientation();

        @Value.Auxiliary
        @Value.Default
        default boolean addToCatalog() {
            return false;
        }
    }
}
