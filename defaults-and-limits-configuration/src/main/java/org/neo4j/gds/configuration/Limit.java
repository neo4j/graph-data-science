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
package org.neo4j.gds.configuration;

import java.util.Locale;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class Limit {
    /**
     * The raw value that was set
     */
    abstract Object getValue();

    /**
     * Violating a limit is type dependent - boolean is the odd one out.
     *
     * We validate the incoming type for compatibility. We support long, double and boolean, the relevant types from
     * Neo4j's procedure framework.
     */
    final boolean isViolated(Object inputValue) {
        assertTypeCompatible(inputValue);

        return isViolatedInternal(inputValue);
    }

    /**
     * Validates the incoming value's type for compatibility
     *
     * @throws java.lang.IllegalArgumentException if the incoming value's type is not compatible.
     */
    private void assertTypeCompatible(Object inputValue) {
        if (inputValue.getClass().isAssignableFrom(getValue().getClass())) return;

        throw new IllegalArgumentException(formatWithLocale(
            "Input value '%s' (%s) is not compatible with limit value '%s' (%s)",
            inputValue,
            inputValue.getClass().getSimpleName().toLowerCase(Locale.ENGLISH),
            getValue(),
            getValue().getClass().getSimpleName().toLowerCase(Locale.ENGLISH)
        ));
    }

    /**
     * Violating a limit is type dependent - boolean is the odd one out.
     */
    protected abstract boolean isViolatedInternal(Object inputValue);

    /**
     * This is handy for creating error messages
     */
    @Deprecated
    abstract String getValueAsString();

    String asErrorMessage(String key, Object value) {
        return formatWithLocale(
            "Configuration parameter '%s' with value '%s' exceeds it's limit of '%s'",
            key,
            value,
            getValue()
        );
    }
}
