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

import org.neo4j.gds.annotation.CustomProcedure;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class ProcedureLookup {

    private final Map<String, ProcedureSpec> procedureSpecs;

    public static ProcedureLookup forPackages(List<String> packages) {
        var procedureSpecs = packages.stream()
            .map(pkg -> new Reflections(pkg, Scanners.MethodsAnnotated)).flatMap(r -> Stream.concat(
                r.getMethodsAnnotatedWith(Procedure.class).stream().map(ProcedureLookup::specFromProcedure),
                r.getMethodsAnnotatedWith(CustomProcedure.class).stream().map(ProcedureLookup::specFromCustomProcedure)
            )).collect(Collectors.toMap(ProcedureSpec::name, s -> s, (keep, first) -> keep));

        return new ProcedureLookup(procedureSpecs);
    }

    private ProcedureLookup(Map<String, ProcedureSpec> procedureSpecs) {
        this.procedureSpecs = procedureSpecs;
    }

    Class<?> findResultType(String fullyQualifiedProcedureName) {
        var spec = tryFindProcedureSpec(fullyQualifiedProcedureName)
            .orElseThrow(() -> unknownProcedure(fullyQualifiedProcedureName));

        return spec.resultType();
    }

    List<String> findArgumentNames(String fullyQualifiedProcedureName) {
        var spec = tryFindProcedureSpec(fullyQualifiedProcedureName)
            .orElseThrow(() -> unknownProcedure(fullyQualifiedProcedureName));

        return spec.argumentNames();
    }

    private static ProcedureSpec specFromProcedure(Method method) {
        var name = method.getAnnotation(Procedure.class).name();
        if (name.isEmpty()) {
            name = method.getAnnotation(Procedure.class).value();
        }
        if (name.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Procedure %s.%s is missing a name.",
                method.getDeclaringClass().getName(),
                method.getName()
            ));
        }

        var resultType = method.getGenericReturnType();
        if (resultType != void.class) {
            if (!(resultType instanceof ParameterizedType)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Procedure %s.%s should return a Stream<T> where T is the actual result type.",
                    method.getDeclaringClass().getName(),
                    method.getName()
                ));
            }
            var actualResultType = ((ParameterizedType) resultType).getActualTypeArguments()[0];
            if (!(actualResultType instanceof Class)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Can't find result class for %s",
                    name
                ));
            }
            resultType = ((Class<?>) actualResultType);
        }

        return ImmutableProcedureSpec.of(name, (Class<?>) resultType, parameterNames(method));
    }

    private static ProcedureSpec specFromCustomProcedure(Method method) {
        var name = method.getAnnotation(CustomProcedure.class).value();
        var resultType = method.getReturnType();
        return ImmutableProcedureSpec.of(name, resultType, parameterNames(method));
    }

    private static List<String> parameterNames(Method method) {
        return Arrays.stream(method.getParameters())
            .map(parameter -> parameter.isAnnotationPresent(Name.class)
                ? parameter.getAnnotation(Name.class).value()
                : parameter.getName())
            .collect(Collectors.toList());
    }

    private Optional<ProcedureSpec> tryFindProcedureSpec(String fullyQualifiedProcedureName) {
        return Optional.ofNullable(this.procedureSpecs.get(fullyQualifiedProcedureName));
    }

    private IllegalArgumentException unknownProcedure(String fullyQualifiedProcedureName) {
        return new IllegalArgumentException(formatWithLocale(
            "Unknown procedure: `%s`",
            fullyQualifiedProcedureName
        ));
    }

    @ValueClass
    interface ProcedureSpec {

        String name();

        Class<?> resultType();

        List<String> argumentNames();
    }
}
