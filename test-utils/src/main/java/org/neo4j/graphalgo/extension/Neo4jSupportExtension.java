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
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.core.EnterpriseLicensingExtension;
import org.neo4j.graphalgo.core.utils.mem.AllocationTrackerExtensionFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.stream;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class Neo4jSupportExtension implements BeforeEachCallback {

    private static final String RETURN_STATEMENT = "RETURN *";

    // taken from org.neo4j.test.extension.DbmsSupportController
    private static final ExtensionContext.Namespace DBMS_NAMESPACE = ExtensionContext.Namespace.create(
        "org",
        "neo4j",
        "dbms"
    );

    // taken from org.neo4j.test.extension.DbmsSupportController
    private static final String DBMS_KEY = "service";

    @ExtensionCallback
    private void configuration(TestDatabaseManagementServiceBuilder builder) {
        builder.impermanent();
        builder.noOpSystemGraphInitializer();
        builder.addExtension(new EnterpriseLicensingExtension());
        builder.addExtension(new AllocationTrackerExtensionFactory());
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        GraphDatabaseAPI db = (GraphDatabaseAPI) getDbms(context)
            .map(dbms -> dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME))
            .orElseThrow(() -> null);

        Class<?> requiredTestClass = context.getRequiredTestClass();
        Map<String, Long> idMapping = neo4jGraphSetup(db, requiredTestClass);
        injectFields(context, db, idMapping);
    }

    private Optional<DatabaseManagementService> getDbms(ExtensionContext context) {
        return Optional.ofNullable(context.getStore(DBMS_NAMESPACE).get(DBMS_KEY, DatabaseManagementService.class));
    }

    private Map<String, Long> neo4jGraphSetup(GraphDatabaseService db, Class<?> testCLass) {
        return stream(testCLass.getDeclaredFields())
            .filter(field -> field.isAnnotationPresent(Neo4jGraph.class))
            .findFirst()
            .map(Neo4jSupportExtension::neo4jGraphSetupForField)
            .map(query -> formatWithLocale("%s %s", query, RETURN_STATEMENT))
            .map(query -> QueryRunner.runQuery(db, query, Neo4jSupportExtension::extractVariableIds))
            .orElseGet(Map::of);
    }

    private static Map<String, Long> extractVariableIds(Result result) {
        if (!result.hasNext()) {
            throw new IllegalArgumentException("Result of create query was empty");
        }
        List<String> columns = result.columns();
        Map<String, Object> row = result.next();

        Map<String, Long> idMapping = new HashMap<>();
        columns.forEach(column -> {
            Object value = row.get(column);
            if (value instanceof NodeEntity) {
                long nodeId = ((NodeEntity) value).getId();
                idMapping.put(column, nodeId);
            }
        });

        return idMapping;
    }

    private static String neo4jGraphSetupForField(Field field) {
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

        String setupQuery;
        try {
            setupQuery = field.get(null).toString();
        } catch(IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return setupQuery;
    }

    private void injectFields(ExtensionContext context, GraphDatabaseAPI db, Map<String, Long> idMapping) {
        IdFunction idFunction = idMapping::get;
        context.getRequiredTestInstances().getAllInstances().forEach(testInstance -> {
            injectInstance(testInstance, idFunction, IdFunction.class);
            injectInstance(testInstance, db, GraphDatabaseAPI.class);
        });
    }

    private static <T> void injectInstance(Object testInstance, T instance, Class<T> clazz) {
        Class<?> testClass = testInstance.getClass();
        do {
            stream(testClass.getDeclaredFields())
                .filter(field -> field.getType() == clazz)
                .filter(field -> isAnnotated(field, org.neo4j.graphalgo.extension.Inject.class))
                .findFirst()
                .ifPresent(field -> setField(testInstance, field, instance));
            testClass = testClass.getSuperclass();
        }
        while (testClass != null);
    }

    private static void setField(Object testInstance, Field field, Object db) {
        field.setAccessible(true);
        try {
            field.set(testInstance, db);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
