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
package org.neo4j.graphalgo.doc.syntax;

import org.neo4j.procedure.Procedure;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

final class ProcedureLookup {

    private static final List<String> PACKAGES_TO_SCAN = List.of(
        "org.neo4j.graphalgo",
        "org.neo4j.gds"
    );

    private final List<Method> procedureMethods;
    private static final ProcedureLookup INSTANCE = new ProcedureLookup();

    private ProcedureLookup() {
        procedureMethods = PACKAGES_TO_SCAN.stream()
            .map(this::createReflections)
            .map(r -> r.getMethodsAnnotatedWith(Procedure.class))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private Method findProcedureMethod(String fullyQualifiedProcedureName) {
        return procedureMethods
            .stream()
            .filter(method -> {
                var annotation = method.getAnnotation(Procedure.class);
                return annotation.name().equals(fullyQualifiedProcedureName) || annotation.value().equals(fullyQualifiedProcedureName);
            })
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(fullyQualifiedProcedureName));
    }

    static Class<?> findResultType(String fullyQualifiedProcedureName) {
        var method = INSTANCE.findProcedureMethod(fullyQualifiedProcedureName);
        var resultType = (ParameterizedType) method.getGenericReturnType();
        var actualTypeArgument = resultType.getActualTypeArguments()[0];
        if (actualTypeArgument instanceof Class) {
            return (Class<?>) actualTypeArgument;
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Can't find result class for %s",
            fullyQualifiedProcedureName
        ));
    }

    private Reflections createReflections(String pkg) {
        return new Reflections(
            pkg,
            new MethodAnnotationsScanner()
        );
    }
}
