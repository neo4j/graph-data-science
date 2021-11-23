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
package org.neo4j.gds.junit.annotation;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;

import java.lang.reflect.AnnotatedElement;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.params.shadow.com.univocity.parsers.annotations.helpers.AnnotationHelper.findAnnotation;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class EnableForNeo4jVersionCondition implements ExecutionCondition {

    private static final ConditionEvaluationResult ENABLED_BY_DEFAULT =
        ConditionEvaluationResult.enabled("@EnableForNeo4jVersion is not present");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        AnnotatedElement element = context
            .getElement()
            .orElseThrow(IllegalStateException::new);

        var runningNeo4jVersion = GraphDatabaseApiProxy.neo4jVersion();

        var single = findAnnotation(element, EnableForNeo4jVersion.class);
        if (single != null && single.value() != runningNeo4jVersion) {
            return disabled(formatWithLocale(
                "Not enabled for %s, only for Neo4j version %s",
                runningNeo4jVersion,
                single.value()
            ));
        }

        return ENABLED_BY_DEFAULT;
    }
}
