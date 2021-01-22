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
package org.neo4j.graphalgo.utils.cypher;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.cypher.internal.v4_0.ast.prettifier.ExpressionStringifier;
import org.neo4j.cypher.internal.v4_0.ast.prettifier.ExpressionStringifier$;
import org.neo4j.cypher.internal.v4_0.expressions.Expression;
import scala.runtime.AbstractFunction1;

public final class CypherPrinter40 implements CypherPrinterApi {

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
        Expression expression = AstHelpers40.any(value);
        if (expression != null) {
            return STRINGIFIER.apply(expression);
        }
        return ifEmpty;
    }

    private static final ExpressionStringifier STRINGIFIER =
        ExpressionStringifier$.MODULE$.apply(
            new CanonicalStringFallback(),
            /* alwaysParens */ false,
            /* alwaysBacktick */ false
        );

    private static final class CanonicalStringFallback extends AbstractFunction1<Expression, String> {

        @Override
        public String apply(Expression expression) {
            return expression.asCanonicalStringVal();
        }
    }
}
