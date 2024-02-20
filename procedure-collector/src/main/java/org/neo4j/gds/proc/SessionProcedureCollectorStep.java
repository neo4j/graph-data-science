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
import java.util.Set;

public class SessionProcedureCollectorStep implements BasicAnnotationProcessor.Step {
    static final String PROCEDURE = Procedure.class.getCanonicalName();
    static final String USER_FUNCTION = UserFunction.class.getCanonicalName();
    static final String USER_AGGREGATION = UserAggregationFunction.class.getCanonicalName();

    private final Set<TypeElement> procedures;
    private final Set<TypeElement> functions;
    private final Set<TypeElement> aggregations;

    SessionProcedureCollectorStep(
        Set<TypeElement> outProcedures,
        Set<TypeElement> outFunctions,
        Set<TypeElement> outAggregations
    ) {
        this.procedures = outProcedures;
        this.functions = outFunctions;
        this.aggregations = outAggregations;
    }

    @Override
    public Set<String> annotations() {
        return Set.of(PROCEDURE, USER_FUNCTION, USER_AGGREGATION);
    }

    @Override
    public Set<? extends Element> process(ImmutableSetMultimap<String, Element> elementsByAnnotation) {
        for (var procedure : elementsByAnnotation.get(PROCEDURE)) {
            if(isInPackage(procedure)) {
                procedures.add(MoreElements.asType(procedure.getEnclosingElement()));
            }
        }

        for (var function : elementsByAnnotation.get(USER_FUNCTION)) {
            if(isInPackage(function)) {
                functions.add(MoreElements.asType(function.getEnclosingElement()));
            }
        }

        for (var aggregation : elementsByAnnotation.get(USER_AGGREGATION)) {
            if(isInPackage(aggregation)) {
                aggregations.add(MoreElements.asType(aggregation.getEnclosingElement()));
            }
        }

        return Set.of();
    }

    private boolean isInPackage(Element element) {
        var thePackage = MoreElements.getPackage(element);
        var packageName = thePackage.getQualifiedName().toString();
        return packageName.startsWith("org.neo4j.gds.") || packageName.equals("org.neo4j.gds")
            || packageName.startsWith("com.neo4j.gds.") || packageName.equals("com.neo4j.gds");
    }
}
