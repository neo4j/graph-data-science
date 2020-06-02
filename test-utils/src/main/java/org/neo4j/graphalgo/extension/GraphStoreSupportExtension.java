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

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.gdl.GDLFactory;
import org.neo4j.graphalgo.gdl.ImmutableGraphCreateFromGDLConfig;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.Arrays.stream;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;
import static org.mockito.internal.util.reflection.FieldSetter.setField;

public class GraphStoreSupportExtension implements BeforeEachCallback {

    @Override
    public void beforeEach(ExtensionContext context) {
        Class<?> requiredTestClass = context.getRequiredTestClass();
        gdlGraphs(requiredTestClass).forEach(setup -> injectGraphStore(setup, context));
    }

    private static List<GDLGraphSetup> gdlGraphs(Class<?> testClass) {
        var gdlGraphs = new ArrayList<GDLGraphSetup>();

        do {
            stream(testClass.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(GDLGraph.class))
                .map(GraphStoreSupportExtension::gdlGraph)
                .forEach(gdlGraphs::add);

            testClass = testClass.getSuperclass();
        } while (testClass != null);

        if (gdlGraphs.isEmpty()) {
            throw new IllegalArgumentException(
                "@GraphStoreExtension annotated classes require at least one field annotated with @GDLGraph.");
        }

        return gdlGraphs;
    }

    private static GDLGraphSetup gdlGraph(Field field) {
        if (field.getType() != String.class) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "@GDLGraph annotated fields must be of type String. Field `%s` has type %s.",
                field, field.getType()));
        }
        String gdlGraph;
        try {
            gdlGraph = field.get(null).toString();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        var annotation = field.getAnnotation(GDLGraph.class);

        return ImmutableGDLGraphSetup.of(
            gdlGraph,
            annotation.graphName(),
            annotation.username(),
            annotation.orientation()
        );
    }

    private static void injectGraphStore(GDLGraphSetup gdlGraphSetup, ExtensionContext context) {
        String graphName = gdlGraphSetup.graphName();

        var createConfig = ImmutableGraphCreateFromGDLConfig.builder()
            .username(gdlGraphSetup.username())
            .graphName(graphName)
            .gdlGraph(gdlGraphSetup.gdlGraph())
            .orientation(gdlGraphSetup.orientation())
            .build();

        GDLFactory gdlFactory = GDLFactory.of(createConfig);
        GraphStore graphStore = gdlFactory.build().graphStore();
        Graph graph = graphStore.getUnion();

        context.getRequiredTestInstances().getAllInstances().forEach(testInstance -> {
            injectInstance(testInstance, graphName, gdlFactory, GDLFactory.class);
            injectInstance(testInstance, graphName, graphStore, GraphStore.class);
            injectInstance(testInstance, graphName, graph, Graph.class);
        });
    }

    private static <T> void injectInstance(Object testInstance, String graphName, T instance, Class<T> clazz) {
        Class<?> testClass = testInstance.getClass();
        do {
            stream(testClass.getDeclaredFields())
                .filter(field -> isAnnotated(field, Inject.class))
                .filter(field -> field.getAnnotation(Inject.class).graphName().equals(graphName))
                .filter(field -> field.getType() == clazz)
                .forEach(field -> setField(testInstance, field, instance));
            testClass = testClass.getSuperclass();
        }
        while (testClass != null);
    }

    @ValueClass
    interface GDLGraphSetup {
        String gdlGraph();

        String graphName();

        String username();

        Orientation orientation();
    }
}
