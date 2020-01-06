/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.cypher.v3_5;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.cypher.internal.v3_5.ast.prettifier.ExpressionStringifier;
import org.neo4j.cypher.internal.v3_5.ast.prettifier.ExpressionStringifier$;
import org.neo4j.cypher.internal.v3_5.expressions.Expression;
import scala.runtime.AbstractFunction1;

import static org.neo4j.graphalgo.cypher.v3_5.AstHelpers.any;

@Value.Style(
    allParameters = true,
    builderVisibility = Value.Style.BuilderVisibility.PUBLIC,
    deepImmutablesDetection = true,
    deferCollectionAllocation = true,
    depluralize = true,
    overshadowImplementation = true,
    typeAbstract = "*",
    typeImmutable = "_*",
    visibility = Value.Style.ImplementationVisibility.PACKAGE
)
public final class CypherPrinter {

    /**
     * Renders any java type as a Cypher expression. Supported types are
     * primitives, CharSequences, Enums, Iterables, and Maps.
     * Empty lists and maps, as well as null, are considered to be "empty"
     * and will be ignored.
     *
     * @return A Cypher expression string for the type or the empty string if the type was empty
     * @throws IllegalArgumentException if the given type is not supported
     */
    public @NotNull String toCypherString(@Nullable Object value) {
        return toCypherStringOr(value, "");
    }

    /**
     * Renders any java type as a Cypher expression. Supported types are
     * primitives, CharSequences, Enums, Iterables, and Maps.
     * Empty lists and maps, as well as null, are considered to be "empty"
     * and will be ignored.
     *
     * @return A Cypher expression string for the type or the given fallback value if the type was empty
     * @throws IllegalArgumentException if the given type is not supported
     */
    public @NotNull String toCypherStringOr(
        @Nullable Object value,
        @NotNull String ifEmpty
    ) {
        Expression expression = any(value);
        if (expression != null) {
            return STRINGIFIER.apply(expression);
        }
        return ifEmpty;
    }

    public CypherParameter parameter(String value) {
        return _CypherParameter.of(value);
    }

    public CypherVariable variable(String value) {
        return _CypherVariable.of(value);
    }

    @Value.Immutable
    interface CypherParameter {
        String name();
    }

    @Value.Immutable
    interface CypherVariable {
        String name();
    }

    private static final ExpressionStringifier STRINGIFIER =
        ExpressionStringifier$.MODULE$.apply(new CanonicalStringFallback());

    private static final class CanonicalStringFallback extends AbstractFunction1<Expression, String> {
        @Override
        public String apply(Expression expression) {
            return expression.asCanonicalStringVal();
        }
    }
}
