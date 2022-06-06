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

import org.neo4j.gds.annotation.ReturnType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class ProcedureLookup {

    @ValueClass
    interface AggregationMethods {
        Method procedure();

        Method update();

        Method result();
    }

    private final List<Method> procedureMethods;
    private final List<AggregationMethods> aggregationMethods;

    public static ProcedureLookup forPackages(List<String> packages) {
        var reflections = packages.stream()
            .map(pkg -> new Reflections(pkg, Scanners.MethodsAnnotated))
            .collect(Collectors.toList());

        var methods = reflections.stream()
            .flatMap(r -> r.getMethodsAnnotatedWith(Procedure.class).stream())
            .collect(Collectors.toList());

        var aggregationMethods = reflections.stream()
            .flatMap(r -> {
                return r.getMethodsAnnotatedWith(UserAggregationFunction.class).stream().map(procedure -> {
                    var aggregatorType = procedure.getReturnType();
                    var update = Arrays
                        .stream(aggregatorType.getDeclaredMethods())
                        .filter(m -> m.isAnnotationPresent(UserAggregationUpdate.class))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException(
                            "UserAggregationFunction without a UserAggregationUpdate"));
                    var result = Arrays
                        .stream(aggregatorType.getDeclaredMethods())
                        .filter(m -> m.isAnnotationPresent(UserAggregationResult.class))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException(
                            "UserAggregationFunction without a UserAggregationResult"));
                    return ImmutableAggregationMethods.of(
                        procedure, update, result
                    );
                });
            }).collect(Collectors.toList());


        return new ProcedureLookup(methods, aggregationMethods);
    }

    private ProcedureLookup(List<Method> procedureMethods, List<AggregationMethods> aggregationMethods) {
        this.procedureMethods = procedureMethods;
        this.aggregationMethods = aggregationMethods;
    }

    Class<?> findResultType(String fullyQualifiedProcedureName) {
        var method = tryFindProcedureMethod(fullyQualifiedProcedureName)
            .or(() -> tryFindAggregationResultMethod(fullyQualifiedProcedureName))
            .orElseThrow(() -> unknownProcedure(fullyQualifiedProcedureName));

        var returnType = method.getAnnotation(ReturnType.class);
        if (returnType != null) {
            return returnType.value();
        }

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

    List<String> findArgumentNames(String fullyQualifiedProcedureName) {
        var method = tryFindProcedureMethod(fullyQualifiedProcedureName)
            .or(() -> tryFindAggregationUpdateMethod(fullyQualifiedProcedureName))
            .orElseThrow(() -> unknownProcedure(fullyQualifiedProcedureName));

        var parameters = method.getParameters();
        var result = new ArrayList<String>(parameters.length);
        for (java.lang.reflect.Parameter parameter : parameters) {
            if (parameter.isAnnotationPresent(Name.class)) {
                result.add(parameter.getAnnotation(Name.class).value());
            } else {
                result.add(parameter.getName());
            }
        }

        return result;
    }

    private Optional<Method> tryFindProcedureMethod(String fullyQualifiedProcedureName) {
        return procedureMethods
            .stream()
            .filter(method -> {
                var annotation = method.getAnnotation(Procedure.class);
                return annotation.name().equals(fullyQualifiedProcedureName) || annotation
                    .value()
                    .equals(fullyQualifiedProcedureName);
            })
            .findFirst();
    }

    private Optional<Method> tryFindAggregationUpdateMethod(String fullyQualifiedProcedureName) {
        return findAggregations(fullyQualifiedProcedureName)
            .map(AggregationMethods::update)
            .findFirst();
    }

    private Optional<Method> tryFindAggregationResultMethod(String fullyQualifiedProcedureName) {
        return findAggregations(fullyQualifiedProcedureName)
            .map(AggregationMethods::result)
            .findFirst();
    }

    private Stream<AggregationMethods> findAggregations(String fullyQualifiedProcedureName) {
        return aggregationMethods
            .stream()
            .filter(aggregation -> {
                var annotation = aggregation.procedure().getAnnotation(UserAggregationFunction.class);
                return annotation.name().equals(fullyQualifiedProcedureName) || annotation
                    .value()
                    .equals(fullyQualifiedProcedureName);
            });
    }

    private IllegalArgumentException unknownProcedure(String fullyQualifiedProcedureName) {
        return new IllegalArgumentException(formatWithLocale(
            "Unknown procedure: `%s`",
            fullyQualifiedProcedureName
        ));
    }
}
