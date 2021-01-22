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
package org.neo4j.graphalgo.junit.annotation;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;

import java.lang.reflect.AnnotatedElement;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.params.shadow.com.univocity.parsers.annotations.helpers.AnnotationHelper.findAnnotation;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class DisableForNeo4jVersionCondition implements ExecutionCondition {

    private static final ConditionEvaluationResult ENABLED_BY_DEFAULT =
        ConditionEvaluationResult.enabled(
            "@DisableForNeo4jVersion is not present");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        AnnotatedElement element = context
            .getElement()
            .orElseThrow(IllegalStateException::new);
        return shouldDisableForNeo4jVersion(
            findAnnotation(element, DisableForNeo4jVersion.class),
            element
        );
    }

    private ConditionEvaluationResult shouldDisableForNeo4jVersion(
        DisableForNeo4jVersion annotation,
        AnnotatedElement element
    ) {
        if (annotation != null) {
            var disableForNeo4jVersion = annotation.value();
            var runningOnNeo4jVersion = GraphDatabaseApiProxy.neo4jVersion();
            if (runningOnNeo4jVersion == disableForNeo4jVersion) {
                var message = annotation.message().isBlank() ?
                    formatWithLocale(
                        "%s should be disabled for Neo4j %s",
                        element.toString(),
                        disableForNeo4jVersion.toString()
                    ) :
                    annotation.message();

                return disabled(message);
            }
        }

        return ENABLED_BY_DEFAULT;
    }
}
