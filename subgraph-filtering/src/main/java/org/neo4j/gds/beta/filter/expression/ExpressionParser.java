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
package org.neo4j.gds.beta.filter.expression;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.opencypher.v9_0.parser.javacc.Cypher;
import org.opencypher.v9_0.parser.javacc.CypherCharStream;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ExpressionParser {

    public static Expression parse(String cypher, Map<String, ValueType> properties) throws ParseException {
        var astFactory = new GdsAstFactory(properties);
        var exceptionFactory = new ExceptionFactory();
        var charstream = new CypherCharStream(cypher);

        var parser = new Cypher<>(
            astFactory,
            exceptionFactory,
            charstream
        );

        var expression = parser.Expression();

        // We need to make sure that the parser consumed the whole input.
        if (!parser.getNextToken().toString().isEmpty()) {
            throw new ParseException(formatWithLocale("Expected a single filter expression, got '%s'", cypher));
        }

        return expression;
    }

    private ExpressionParser() {}

}
