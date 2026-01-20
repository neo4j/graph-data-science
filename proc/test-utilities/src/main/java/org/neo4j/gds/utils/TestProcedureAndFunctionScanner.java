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

import org.neo4j.gds.ProcedureAndFunctionScanner;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public final class TestProcedureAndFunctionScanner {

    private TestProcedureAndFunctionScanner() {}


    public static Class<?>[] procedures() {
        return classesContainingAnnotation(Procedure.class);
    }

    public static Class<?>[] functions() {
        return classesContainingAnnotation(UserFunction.class);
    }

    public static Class<?>[] aggregationFunctions() {
        return classesContainingAnnotation(UserAggregationFunction.class);
    }

    private static Class<?>[] classesContainingAnnotation(Class<? extends Annotation> annotation) {
        return ProcedureAndFunctionScanner.streamMethodsContainingAnnotation(annotation)
            .map(Method::getDeclaringClass)
            .filter(declaringClass -> !declaringClass.getPackage().getName().startsWith("org.neo4j.gds.test"))
            .distinct()
            .toArray(Class[]::new);
    }
}
