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

import org.neo4j.gds.utils.StringJoining;
import org.opencypher.v9_0.ast.factory.ASTExceptionFactory;

import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ExceptionFactory implements ASTExceptionFactory {

    @Override
    public Exception syntaxException(
        String got,
        List<String> expected,
        Exception source,
        int offset,
        int line,
        int column
    ) {
        var message = formatWithLocale(
            "Invalid input '%s': expected %s",
            got,
            expected.size() == 1 ? expected.stream().findFirst().get() : StringJoining.join(expected)
        );

        return new IllegalArgumentException(message);
    }

    @Override
    public Exception syntaxException(Exception source, int offset, int line, int column) {
        return new Exception(source);
    }
}
