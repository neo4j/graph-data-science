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
package org.neo4j.gds.doc.syntax;

import org.apache.commons.lang3.reflect.MethodUtils;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.utils.StringFormatting;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

// based on parser used for gds-api-spec
public record ConfigurationParser(Class<?> configClass) {

    private static final Set<Class<?>> IGNORED_METHOD_ANNOTATIONS = Set.of(
        Configuration.Ignore.class,
        Configuration.Check.class,
        Configuration.CollectKeys.class,
        Configuration.ToMap.class,
        Configuration.GraphStoreValidation.class,
        Configuration.GraphStoreValidationCheck.class,
        Configuration.Parameter.class
    );

    List<ParameterSpec> parse() {
        return Arrays.stream(configClass.getMethods())
            .filter(m -> !Modifier.isStatic(m.getModifiers()))
            .filter(m -> Modifier.isPublic(m.getModifiers()))
            .filter(
                m -> Arrays.stream(m.getAnnotations())
                    .map(Annotation::annotationType)
                    .noneMatch(IGNORED_METHOD_ANNOTATIONS::contains)
            )
            .map(m -> new ParameterSpec(getArgumentName(m), getDefaultValue(m, configClass), getArgumentType(m)))
            .toList();
    }

    private String getArgumentName(Method method) {
        var maybeKeyAnnotation = method.getDeclaredAnnotation(Configuration.Key.class);
        return maybeKeyAnnotation != null ? maybeKeyAnnotation.value() : method.getName();
    }

    private ParameterTypeSpec getArgumentType(Method method) {
        Class<?> returnType = method.getReturnType();

        var optional = method.isDefault() || Optional.class.isAssignableFrom(returnType);

        Configuration.ConvertWith convertWith;
        if ((convertWith = method.getDeclaredAnnotation(Configuration.ConvertWith.class)) != null) {
            return new ParameterTypeSpec(getParameterTypeFromConvertWith(method, convertWith), optional);
        }


        if (returnType.isPrimitive()) {
            return new ParameterTypeSpec(returnType.getSimpleName(), optional);
        }

        if (Optional.class.isAssignableFrom(returnType)) {
            var innerType = getGenericReturnType(method);
            return new ParameterTypeSpec(innerType, true);
        }

        if (Collection.class.isAssignableFrom(returnType)) {
            var innerType = getGenericReturnType(method);
            return new ParameterTypeSpec(StringFormatting.formatWithLocale("list<%s>", innerType), optional);
        }

        return new ParameterTypeSpec(returnType.getSimpleName(), optional);
    }

    private static @NotNull String getParameterTypeFromConvertWith(
        Method method,
        Configuration.ConvertWith convertWith
    ) {
        var conversionName = convertWith.method();

        String conversionClassName;
        String conversionMethodName;

        var classMethodSplit = conversionName.split("#");
        if (classMethodSplit.length != 2) {
            conversionClassName = method.getDeclaringClass().getName();
            conversionMethodName = conversionName;
        } else {
            conversionClassName = classMethodSplit[0];
            conversionMethodName = classMethodSplit[1];
        }

        try {
            var conversionClass = lookupClass(conversionClassName);
            var conversionMethod = Arrays.stream(conversionClass.getDeclaredMethods())
                .filter(m -> m.getName().equals(conversionMethodName))
                .findFirst()
                .orElseThrow();
            return conversionMethod.getParameters()[0].getType().getSimpleName();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(StringFormatting.formatWithLocale("Could not find class %s", conversionClassName));
        }
    }

    private Optional<Object> getDefaultValue(Method method, Class<?> configClass) {
        if (!method.isDefault()) {
            return Optional.empty();
        }

        var proxy = Proxy.newProxyInstance(
            configClass.getClassLoader(),
            new Class<?>[]{configClass},
            (proxy1, method1, args) -> null
        );

        try {
            var defaultValue = MethodHandles.lookup()
                .unreflectSpecial(method, configClass)
                .bindTo(proxy)
                .invoke();

            if (method.isAnnotationPresent(Configuration.ToMapValue.class) && defaultValue != null) {
                var toMapValue = method.getAnnotation(Configuration.ToMapValue.class);
                defaultValue = convertDefaultValueWithToMapValue(toMapValue.value(), defaultValue);
            }

            return Optional.ofNullable(defaultValue);
        } catch (Throwable e) {
            throw new RuntimeException(StringFormatting.formatWithLocale("Could not get default method for %s. %s", method.getName(), e));
        }
    }

    private Optional<Object> convertDefaultValueWithToMapValue(String toMapValueMethod, Object value) {
        try {
            Class<?> clazz;
            String methodName;

            if (toMapValueMethod.contains("#")) {
                String[] parts = toMapValueMethod.split("#");
                clazz = lookupClass(parts[0]);
                methodName = parts[1];

            } else {
                clazz = this.configClass;
                methodName = toMapValueMethod;
            }

            return Optional.ofNullable(MethodUtils.invokeStaticMethod(clazz, methodName, value));
        } catch (Throwable e) {
            throw new RuntimeException(
                StringFormatting.formatWithLocale("Could not invoke toMapValue for %s, %s", toMapValueMethod, e.getCause())
            );
        }
    }


    private static @NotNull Class<?> lookupClass(String conversionClassName) throws ClassNotFoundException {
        try {
            return Class.forName(conversionClassName);
        } catch (ClassNotFoundException e) {
            // The referenced class could be an inner class. We need to replace the last `.` with a `$`
            String innerClassName = conversionClassName.replaceAll("\\.(?=[^.]*$)", "\\$");
            return Class.forName(innerClassName);
        }
    }

    private static String getGenericReturnType(Method method) {
        var innerType = (ParameterizedType) method.getGenericReturnType();
        var innerTypeName = innerType.getActualTypeArguments()[0].getTypeName();
        return innerTypeName.split("\\.")[innerTypeName.split("\\.").length - 1];
    }
}
