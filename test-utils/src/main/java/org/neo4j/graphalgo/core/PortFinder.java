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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class PortFinder implements ParameterResolver, TestInstancePostProcessor {

    @Target({ElementType.PARAMETER, ElementType.TYPE, ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface FreePort {
        int preferred() default PREFERRED_PORT;
    }

    @Inherited
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @ExtendWith(PortFinder.class)
    public @interface Extension {
    }

    public static final int PREFERRED_PORT = 1337;
    public static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create("gds-ee");

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    throws ParameterResolutionException {
        if (parameterContext.isAnnotated(FreePort.class)) {
            return parameterContext.getParameter().getType().isAssignableFrom(int.class);
        }
        return false;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    throws ParameterResolutionException {
        return freePortFromStore(extensionContext, parameterContext.findAnnotation(FreePort.class).get());
    }

    @Override
    public void postProcessTestInstance(Object testInstance, ExtensionContext context) {
        Stream.<Class<?>>iterate(testInstance.getClass(), Objects::nonNull, Class::getSuperclass)
            .flatMap(c -> Arrays.stream(c.getDeclaredFields()))
            .filter(c -> c.isAnnotationPresent(FreePort.class))
            .filter(c -> c.getType().isAssignableFrom(int.class))
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
                    formatWithLocale("Field %s cannot be set, please make it either public or accessible to reflection.", field
                        .getName()));
            }
        }
    }

    private void trySetValueToField(Object testInstance, ExtensionContext context, Field field) throws IllegalAccessException {
        var fieldContext = Modifier.isStatic(field.getModifiers()) ? null : testInstance;
        var existingValue = field.get(fieldContext);

        if (existingValue != null && !Objects.equals(existingValue, 0)) {
            throw new ExtensionConfigurationException(
                formatWithLocale("Field %s should not have any manually assigned value.", field.getName()));
        }

        var annotation = field.getAnnotation(FreePort.class);
        var value = freePortFromStore(context, annotation);

        field.set(fieldContext, value);
    }

    private int freePortFromStore(ExtensionContext extensionContext, FreePort annotation) {
        var store = extensionContext.getStore(NAMESPACE);
        return store.getOrComputeIfAbsent(
            FreePort.class,
            (annotationClass) -> freePort(annotation.preferred()),
            int.class
        );
    }

    public static int freePort() {
        return freePort(PREFERRED_PORT);
    }

    private static int freePort(int preferred) {
        var random = new Random();
        return IntStream.concat(
            IntStream.of(preferred),
            random.ints(1024, 65536)
        )
            .filter(PortFinder::portAvailable)
            .findFirst()
            .orElse(0);
    }

    private static boolean portAvailable(int port) {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getLocalHost(), port), 1);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
