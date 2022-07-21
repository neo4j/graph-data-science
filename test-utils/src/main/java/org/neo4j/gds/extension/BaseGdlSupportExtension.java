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
package org.neo4j.gds.extension;

import org.immutables.value.Value;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.gdl.ImmutableGraphProjectFromGdlConfig;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;
import static org.neo4j.gds.extension.ExtensionUtil.getStringValueOfField;
import static org.neo4j.gds.extension.ExtensionUtil.setField;

abstract class BaseGdlSupportExtension {

    public static final NamedDatabaseId DATABASE_ID = DatabaseIdFactory.from("GDL", UUID.fromString("42-42-42-42-42"));
    public static final DatabaseId NEW_DATABASE_ID = DatabaseId.from("GDL");

    void beforeAction(ExtensionContext context) {
        Class<?> requiredTestClass = context.getRequiredTestClass();
        gdlGraphs(requiredTestClass).forEach(setup -> injectGraphStore(setup, context));
    }

    void afterAction(ExtensionContext context) {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    private static Collection<GdlGraphSetup> gdlGraphs(Class<?> testClass) {
        var setups = new HashSet<GdlGraphSetup>();

        do {
            stream(testClass.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(GdlGraph.class) || f.isAnnotationPresent(GdlGraphs.class))
                .flatMap(BaseGdlSupportExtension::gdlGraphsForField)
                .peek(setup -> {
                    if (setups.contains(setup)) {
                        throw new ExtensionConfigurationException(String.format(
                            Locale.ENGLISH,
                            "Graph names prefixes must be unique. Found duplicate graph name prefixes '%s'.",
                            setup.graphNamePrefix()
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
        String gdl = getStringValueOfField(field);

        var annotations = field.isAnnotationPresent(GdlGraph.class)
            ? Stream.of(field.getAnnotation(GdlGraph.class))
            : stream(field.getAnnotation(GdlGraphs.class).value());

        return annotations
            .map(annotation -> ImmutableGdlGraphSetup.builder()
                .graphNamePrefix(annotation.graphNamePrefix())
                .gdlGraph(gdl)
                .username(annotation.username())
                .orientation(annotation.orientation())
                .aggregation(annotation.aggregation())
                .idOffset(annotation.idOffset())
                .addToCatalog(annotation.addToCatalog())
                .build()
            );
    }

    private static void injectGraphStore(GdlGraphSetup gdlGraphSetup, ExtensionContext context) {
        String graphNamePrefix = gdlGraphSetup.graphNamePrefix();
        String graphName = graphNamePrefix.isBlank() ? "graph" : graphNamePrefix + "Graph";

        var graphProjectConfig = ImmutableGraphProjectFromGdlConfig.builder()
            .username(gdlGraphSetup.username())
            .graphName(graphName)
            .gdlGraph(gdlGraphSetup.gdlGraph())
            .orientation(gdlGraphSetup.orientation())
            .aggregation(gdlGraphSetup.aggregation())
            .build();

        var nodeIdFunction = new TestSupport.OffsetIdSupplier(gdlGraphSetup.idOffset());

        GdlFactory gdlFactory = GdlFactory
            .builder()
            .nodeIdFunction(nodeIdFunction)
            .graphProjectConfig(graphProjectConfig)
            .databaseId(NEW_DATABASE_ID)
            .build();

        CSRGraphStore graphStore = gdlFactory.build();
        CSRGraph graph = graphStore.getUnion();
        IdFunction idFunction = gdlFactory::nodeId;
        TestGraph testGraph = new TestGraph(graph, idFunction, graphName);

        if (gdlGraphSetup.addToCatalog()) {
            GraphStoreCatalog.set(graphProjectConfig, graphStore);
        }

        context.getRequiredTestInstances().getAllInstances().forEach(testInstance -> {
            injectInstance(testInstance, graphNamePrefix, graph, Graph.class, "Graph");
            injectInstance(testInstance, graphNamePrefix, graph, CSRGraph.class, "Graph");
            injectInstance(testInstance, graphNamePrefix, testGraph, TestGraph.class, "Graph");
            injectInstance(testInstance, graphNamePrefix, graphStore, GraphStore.class, "GraphStore");
            injectInstance(testInstance, graphNamePrefix, idFunction, IdFunction.class, "IdFunction");
        });
    }

    private static <T> void injectInstance(
        Object testInstance,
        String graphNamePrefix,
        T instance,
        Class<T> clazz,
        String suffix
    ) {
        Stream.<Class<?>>iterate(testInstance.getClass(), Objects::nonNull, Class::getSuperclass)
            .flatMap(c -> Arrays.stream(c.getDeclaredFields()))
            .filter(field -> field.getType() == clazz)
            .filter(field -> isAnnotated(field, Inject.class))
            .filter(field -> field.getName().equalsIgnoreCase(graphNamePrefix + suffix))
            .forEach(field -> setField(testInstance, field, instance));
    }

    @ValueClass
    interface GdlGraphSetup {
        String graphNamePrefix();

        @Value.Auxiliary
        String gdlGraph();

        @Value.Auxiliary
        String username();

        @Value.Auxiliary
        Orientation orientation();

        @Value.Auxiliary
        Aggregation aggregation();

        @Value.Auxiliary
        long idOffset();

        @Value.Auxiliary
        @Value.Default
        default boolean addToCatalog() {
            return false;
        }
    }
}
