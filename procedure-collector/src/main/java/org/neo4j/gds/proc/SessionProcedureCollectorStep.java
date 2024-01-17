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
package org.neo4j.gds.proc;

import com.google.auto.common.BasicAnnotationProcessor;
import com.google.auto.common.MoreElements;
import com.google.common.collect.ImmutableSetMultimap;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;

import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SessionProcedureCollectorStep implements BasicAnnotationProcessor.Step {
    static final String PROCEDURE = Procedure.class.getCanonicalName();
    static final String USER_FUNCTION = UserFunction.class.getCanonicalName();
    static final String USER_AGGREGATION = UserAggregationFunction.class.getCanonicalName();

    private final List<TypeElement> procedures;
    private final List<TypeElement> functions;
    private final List<TypeElement> aggregations;
    private final Set allowedProcedures;

    SessionProcedureCollectorStep(
        List<TypeElement> outProcedures,
        List<TypeElement> outFunctions,
        List<TypeElement> outAggregations,
        Set allowedProcedures
    ) {
        this.procedures = outProcedures;
        this.functions = outFunctions;
        this.aggregations = outAggregations;
        this.allowedProcedures = allowedProcedures;
    }

    @Override
    public Set<String> annotations() {
        return Set.of(PROCEDURE, USER_FUNCTION, USER_AGGREGATION);
    }

    @Override
    public Set<? extends Element> process(ImmutableSetMultimap<String, Element> elementsByAnnotation) {
        var allProcedures = elementsByAnnotation.get(PROCEDURE);
        for (var procedure : allProcedures) {
            if(isInPackage(procedure)) {
                var annotation = Arrays.stream(procedure.getAnnotationsByType(Procedure.class)).toList().get(0);
                if (isAllowed(annotation.value().isEmpty() ? annotation.name() : annotation.value())) {
                    procedures.add(MoreElements.asType(procedure.getEnclosingElement()));
                }
            }
        }

        var allFunctions = elementsByAnnotation.get(USER_FUNCTION);
        for (var function : allFunctions) {
            if(isInPackage(function)) {
                var annotation = Arrays.stream(function.getAnnotationsByType(UserFunction.class)).toList().get(0);
                if (isAllowed(annotation.value().isEmpty() ? annotation.name() : annotation.value())) {
                    functions.add(MoreElements.asType(function.getEnclosingElement()));
                }
            }
        }

        var allAggregations = elementsByAnnotation.get(USER_AGGREGATION);
        for (var aggregation : allAggregations) {
            if(isInPackage(aggregation)) {
                var annotation = Arrays.stream(aggregation.getAnnotationsByType(UserAggregationFunction.class)).toList().get(0);
                if (isAllowed(annotation.value().isEmpty() ? annotation.name() : annotation.value())) {
                    aggregations.add(MoreElements.asType(aggregation.getEnclosingElement()));
                }
            }
        }

        return new HashSet<>();
    }

    private boolean isInPackage(Element element) {
        var thePackage = MoreElements.getPackage(element);
        var packageName = thePackage.getQualifiedName().toString();
        return packageName.startsWith("org.neo4j.gds.");
    }

    private boolean isAllowed(String elementName) {
        return allowedProcedures.contains(elementName);
    }

}
