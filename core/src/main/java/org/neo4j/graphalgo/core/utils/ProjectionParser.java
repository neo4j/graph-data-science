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
package org.neo4j.graphalgo.core.utils;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.petitparser.context.Result;
import org.petitparser.parser.Parser;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.petitparser.parser.primitive.CharacterParser.digit;
import static org.petitparser.parser.primitive.CharacterParser.letter;
import static org.petitparser.parser.primitive.CharacterParser.of;
import static org.petitparser.utils.Functions.withoutSeparators;


public final class ProjectionParser {

    private static final Parser PARSER;

    static {
        Parser underscore = of('_');
        Parser backtick = of('`');
        Parser pipe = of('|');
        Parser colon = of(':');
        Parser validTokenCharacter = letter().or(underscore, digit("expected letter, digit, or underscore"));
        Parser unescapedIdentifier = validTokenCharacter.plus().flatten();
        Parser escapedIdentifier = backtick.seq(backtick.neg().plus().flatten()).seq(backtick).pick(1);
        Parser identifier = colon.optional().seq(escapedIdentifier.or(unescapedIdentifier)).pick(1);
        PARSER = identifier.separatedBy(pipe.trim()).map(withoutSeparators()).end("expected letter, digit, or underscore");
    }

    public static Set<String> parse(@Nullable String projection) {
        if (projection == null || projection.isEmpty()) {
            return Collections.emptySet();
        }
        Result result = PARSER.parse(projection);
        if (result.isSuccess()) {
            List<String> types = result.get();
            return new LinkedHashSet<>(types);
        }
        int errorPos = result.getPosition();
        String errorPointer = "^";
        errorPointer = StringUtils.leftPad(errorPointer, errorPos + 1, '~');
        errorPointer = StringUtils.rightPad(errorPointer, projection.length(), '~');

        throw new IllegalArgumentException(String.format(
                "Could not parse projection: %s (at position %d):%n%s%n%s",
                result.getMessage(), errorPos, projection, errorPointer));
    }

    private ProjectionParser() {
        throw new UnsupportedOperationException("No instances");
    }
}
