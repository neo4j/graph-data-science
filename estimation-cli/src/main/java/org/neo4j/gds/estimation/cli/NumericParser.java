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
package org.neo4j.gds.estimation.cli;

import java.util.regex.Pattern;

import static java.util.Objects.requireNonNullElse;

final class NumericParser {

    private static final Pattern NUMBER_FORMAT = Pattern.compile(
        "(?<num>\\d+)(?<nums>(?:_?\\d+)*)(?:\\s*(?<suffix>[kKmMgGbBtT]))?"
    );
    private static final Pattern SUB_NUMBER_FORMAT = Pattern.compile("_?(?<num>\\d+)");

    static String normalize(String number) {
        var longFormat = NUMBER_FORMAT.matcher(number);
        if (!longFormat.matches()) {
            return number;
        }

        var input = new StringBuilder(number.length());
        input.append(longFormat.group("num"));
        var separatedNumbers = longFormat.group("nums");
        if (!separatedNumbers.isEmpty()) {
            var separations = SUB_NUMBER_FORMAT.matcher(separatedNumbers);
            while (separations.find()) {
                input.append(separations.group("num"));
            }
        }
        switch (requireNonNullElse(longFormat.group("suffix"), "")) {
            case "k":
            case "K":
                input.append("000");
                break;
            case "m":
            case "M":
                input.append("000000");
                break;
            case "g":
            case "G":
            case "b":
            case "B":
                input.append("000000000");
                break;
            case "t":
            case "T":
                input.append("000000000000");
                break;
            default:
        }
        // hue hue hue
        // switch (requireNonNullElse(longFormat.group("suffix"), "")) {
        //     case "t":
        //     case "T":
        //         input.append("000");
        //     case "g":
        //     case "G":
        //     case "b":
        //     case "B":
        //         input.append("000");
        //     case "m":
        //     case "M":
        //         input.append("000");
        //     case "k":
        //     case "K":
        //         input.append("000");
        //     default:
        // }
        return input.toString();
    }
}
