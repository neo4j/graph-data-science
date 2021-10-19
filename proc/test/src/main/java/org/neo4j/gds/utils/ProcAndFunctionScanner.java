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
package org.neo4j.gds.utils;

import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

public class ProcAndFunctionScanner {
    private static final List<String> PACKAGES_TO_SCAN = List.of(
        "com.neo4j.gds",
        "org.neo4j.gds"
    );

    private static final List<Reflections> reflections = PACKAGES_TO_SCAN
        .stream()
        .map(pkg -> new Reflections(pkg, new MethodAnnotationsScanner()))
        .collect(Collectors.toList());


    public static Class<?>[] procedures() throws Exception {
        return classesContainingAnnotation(Procedure.class);
    }

    public static Class<?>[] functions() throws Exception {
        return classesContainingAnnotation(UserFunction.class);
    }

    public static Class<?>[] aggregationFunctions() throws Exception {
        return classesContainingAnnotation(UserAggregationFunction.class);
    }

    private static Class<?>[] classesContainingAnnotation(Class<? extends Annotation> annotation) throws Exception {
        return reflections.stream().flatMap(reflection ->
                reflection
                    .getMethodsAnnotatedWith(annotation)
                    .stream()
                    .map(Method::getDeclaringClass)
                    .filter(declaringClass -> !declaringClass.getPackage().getName().startsWith("org.neo4j.gds.test"))
            )
            .distinct()
            .toArray(Class[]::new);
    }
}
