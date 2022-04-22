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
package org.neo4j.gds.utils;

import org.apache.commons.lang3.StringUtils;
import org.intellij.lang.annotations.PrintFormat;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

public final class StringFormatting {

    private StringFormatting() {}

    public static String formatWithLocale(@PrintFormat String template, Object... inputs) {
        return String.format(Locale.ENGLISH, template, inputs);
    }

    public static String formatNumber(long number) {
        var formatter = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
        var symbols = formatter.getDecimalFormatSymbols();
        symbols.setGroupingSeparator('_');
        formatter.setDecimalFormatSymbols(symbols);
        return formatter.format(number);
    }

    public static String formatNumber(int number) {
        return formatNumber((long) number);
    }

    public static String toLowerCaseWithLocale(String string) {
        return string.toLowerCase(Locale.ENGLISH);
    }

    public static String toUpperCaseWithLocale(String string) {
        return string.toUpperCase(Locale.ENGLISH);
    }

    public static boolean isEmpty(String string) {
        return StringUtils.isEmpty(string);
    }
}
