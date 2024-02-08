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
package org.neo4j.gds.assertj;

import org.assertj.core.api.iterable.ThrowingExtractor;

import java.util.regex.Pattern;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class Extractors {

    private static final Pattern TIME_MEASUREMENTS_PATTERN = Pattern.compile("(\\d+\\s*)(ms|s|min)");

    private Extractors() {}

    public static ThrowingExtractor<String, String, RuntimeException> removingServerAddress() {
        return Extractors::removingServerAddress;
    }

    private static String removingServerAddress(String message) {
        if (message.contains("localhost:")) {
            return removingServerAddress(message.replaceFirst("localhost:\\d+", "<address>"));
        }
        return message;
    }

    public static ThrowingExtractor<String, String, RuntimeException> removingThreadId() {
        return Extractors::removeThreadId;
    }

    private static String removeThreadId(String message) {
        if (message.contains("] ")) {
            return removeThreadId(message.substring(message.indexOf("] ") + 2));
        }
        return message;
    }

    public static ThrowingExtractor<String, String, RuntimeException> replaceTimings() {
        return message -> TIME_MEASUREMENTS_PATTERN.matcher(message).replaceAll("`some time`");
    }

    public static ThrowingExtractor<String, String, RuntimeException> keepingFixedNumberOfDecimals(int decimalPrecision) {
        var pattern = Pattern.compile(formatWithLocale("(\\d+\\.\\d{1,%d})\\d*", decimalPrecision));
        return msg -> pattern.matcher(msg).replaceAll("$1");
    }
}
