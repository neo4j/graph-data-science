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
package org.neo4j.gds.core;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.OpenModelCatalog;

import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class TestOpenModelCatalogResolver implements ParameterResolver, TestInstancePostProcessor, AfterEachCallback {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create("gds");

    private static final Class<InjectModelCatalog> INJECT_CLASS = InjectModelCatalog.class;
    private static final Class<ModelCatalog> INJECT_TARGET_CLASS = ModelCatalog.class;

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        var store = context.getStore(NAMESPACE);
        var modelCatalog = store.get(INJECT_CLASS, INJECT_TARGET_CLASS);
        if (modelCatalog == null) {
            throw new IllegalStateException("No ModelCatalog injected, but used @ModelCatalogExtension");
        }
        modelCatalog.removeAllLoadedModels();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    throws ParameterResolutionException {
        if (parameterContext.isAnnotated(INJECT_CLASS)) {
            return parameterContext.getParameter().getType().isAssignableFrom(INJECT_TARGET_CLASS);
        }
        return false;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    throws ParameterResolutionException {
        return modelCatalogFromExtensionStore(
            extensionContext,
            parameterContext.findAnnotation(INJECT_CLASS).get()
        );
    }

    @Override
    public void postProcessTestInstance(Object testInstance, ExtensionContext context) {
        Stream.<Class<?>>iterate(testInstance.getClass(), Objects::nonNull, Class::getSuperclass)
            .flatMap(c -> Arrays.stream(c.getDeclaredFields()))
            .filter(c -> c.isAnnotationPresent(INJECT_CLASS))
            .filter(c -> c.getType().isAssignableFrom(INJECT_TARGET_CLASS))
            .forEach(field -> setValueToField(testInstance, context, field));
    }

    private void setValueToField(Object testInstance, ExtensionContext context, Field field) {
        try {
            trySetValueToField(testInstance, context, field);
        } catch (IllegalAccessException e) {
            try {
                field.setAccessible(true);
                trySetValueToField(testInstance, context, field);
            } catch (SecurityException | InaccessibleObjectException | IllegalAccessException illegalAccessException) {
                throw new ExtensionConfigurationException(
                    formatWithLocale(
                        "Field %s cannot be set, please make it either public or accessible to reflection.",
                        field.getName()
                    ));
            }
        }
    }

    private void trySetValueToField(Object testInstance, ExtensionContext extensionContext, Field field)
    throws IllegalAccessException {
        var testObject = Modifier.isStatic(field.getModifiers()) ? null : testInstance;
        var existingValue = field.get(testObject);

        if (existingValue != null) {
            throw new ExtensionConfigurationException(
                formatWithLocale("Field %s should not have any manually assigned value.", field.getName()));
        }

        var annotation = field.getAnnotation(INJECT_CLASS);
        var value = modelCatalogFromExtensionStore(extensionContext, annotation);

        field.set(testObject, value);
    }

    private ModelCatalog modelCatalogFromExtensionStore(
        ExtensionContext extensionContext,
        InjectModelCatalog annotation
    ) {
        var store = extensionContext.getStore(NAMESPACE);
        return store.getOrComputeIfAbsent(
            INJECT_CLASS,
            (annotationClass) -> new OpenModelCatalog(),
            INJECT_TARGET_CLASS
        );
    }
}
