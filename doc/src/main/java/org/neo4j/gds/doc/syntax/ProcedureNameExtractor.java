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

import java.util.regex.Pattern;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

final class ProcedureNameExtractor {
    private static final Pattern PATTERN = Pattern.compile("(\\sgds\\.)(\\w+\\.)+(\\w+)");

    private ProcedureNameExtractor() {}

    public static String findProcedureName(String codeSnippet) {
        var matcher = PATTERN.matcher(codeSnippet);
        var matchesFound = matcher.find();
        if(matchesFound) {
            return matcher.group().trim();
        }

        throw new IllegalArgumentException(formatWithLocale("No procedure names found in: \n%s", codeSnippet));
    }

}
