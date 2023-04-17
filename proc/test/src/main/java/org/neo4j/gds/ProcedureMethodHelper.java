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
package org.neo4j.gds;

import org.neo4j.procedure.Procedure;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

public final class ProcedureMethodHelper {

    public static String methodName(Method method) {
        Procedure procedureAnnotation = method.getDeclaredAnnotation(Procedure.class);
        Objects.requireNonNull(procedureAnnotation, method + " is not annotation with " + Procedure.class);
        String name = procedureAnnotation.name();
        if (name.isEmpty()) {
            name = procedureAnnotation.value();
        }
        return name;
    }

    public static Stream<Method> nonEstimateMethods(AlgoBaseProc<?, ?, ?, ?> proc) {
        return methods(proc).filter(procMethod -> !methodName(procMethod).endsWith(".estimate"));
    }

    public static Stream<Method> writeMethods(AlgoBaseProc<?, ?, ?, ?> proc) {
        return methods(proc).filter(procMethod -> methodName(procMethod).endsWith(".write"));
    }

    public static Stream<Method> mutateMethods(AlgoBaseProc<?, ?, ?, ?> proc) {
        return methods(proc).filter(procMethod -> methodName(procMethod).endsWith(".mutate"));
    }

    public static Stream<Method> streamMethods(AlgoBaseProc<?, ?, ?, ?> proc) {
        return methods(proc).filter(procMethod -> methodName(procMethod).endsWith(".stream"));
    }

    public static Stream<Method> statsMethods(AlgoBaseProc<?, ?, ?, ?> proc) {
        return methods(proc).filter(procMethod -> methodName(procMethod).endsWith(".stats"));
    }

    private static Stream<Method> methods(AlgoBaseProc<?, ?, ?, ?> proc) {
        return Arrays.stream(proc.getClass().getDeclaredMethods())
            .filter(method -> method.getDeclaredAnnotation(Procedure.class) != null);
    }

    private ProcedureMethodHelper() {}
}
