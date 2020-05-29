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
import org.neo4j.test.extension.Inject;

import java.lang.reflect.Field;
import java.util.Optional;

import static java.util.Arrays.stream;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;
import static org.mockito.internal.util.reflection.FieldSetter.setField;

public class GraphStoreSupportExtension implements BeforeEachCallback {

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        Class<?> requiredTestClass = context.getRequiredTestClass();

        GDLGraphSetup gdlGraphSetup = gdlGraph(requiredTestClass);

        injectGraphStore(gdlGraphSetup, context);
    }

    private static GDLGraphSetup gdlGraph(Class<?> testClass) throws IllegalAccessException {
        boolean found;
        Optional<Field> maybeField;

        do {
            maybeField = stream(testClass.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(GDLGraph.class))
                .findFirst();
            found = maybeField.isPresent();
            testClass = testClass.getSuperclass();
        } while (testClass != null && !found);

        var field = maybeField.orElseThrow(() -> new IllegalArgumentException("Missing GDLGraph annotation."));

        if (field.getType() != String.class) {
            throw new IllegalArgumentException("Field `" + field.getName() + "` must be of type String.");
        }

        var gdlGraph = field.get(null).toString();
        var annotation = field.getAnnotation(GDLGraph.class);

        return ImmutableGDLGraphSetup.of(
            gdlGraph,
            annotation.graphName(),
            annotation.username(),
            annotation.orientation()
        );
    }

    private static void injectGraphStore(GDLGraphSetup gdlGraphSetup, ExtensionContext context) {
        var createConfig = ImmutableGraphCreateFromGDLConfig.builder()
            .username(gdlGraphSetup.username())
            .graphName(gdlGraphSetup.graphName())
            .gdlGraph(gdlGraphSetup.gdlGraph())
            .orientation(gdlGraphSetup.orientation())
            .build();

        GDLFactory gdlFactory = GDLFactory.of(createConfig);
        GraphStore graphStore = gdlFactory.build().graphStore();
        Graph graph = graphStore.getUnion();

        context.getRequiredTestInstances().getAllInstances().forEach(testInstance -> {
            injectInstance(testInstance, gdlFactory, GDLFactory.class);
            injectInstance(testInstance, graphStore, GraphStore.class);
            injectInstance(testInstance, graph, Graph.class);
        });
    }

    private static <T> void injectInstance(Object testInstance, T instance, Class<T> clazz) {
        Class<?> testClass = testInstance.getClass();
        do {
            stream(testClass.getDeclaredFields())
                .filter(field -> isAnnotated(field, Inject.class))
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
