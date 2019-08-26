/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.values.storable.Values;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadHelperTest {

    @Test
    void extractValueReadsAnyNumericType() {
        assertEquals(42.0, ReadHelper.extractValue(Values.byteValue((byte) 42), 0.0));
        assertEquals(42.0, ReadHelper.extractValue(Values.shortValue((short) 42), 0.0));
        assertEquals(42.0, ReadHelper.extractValue(Values.intValue(42), 0.0));
        assertEquals(42.0, ReadHelper.extractValue(Values.longValue(42), 0.0));
        assertEquals(42.0, ReadHelper.extractValue(Values.floatValue(42.0F), 0.0));
        assertEquals(42.0, ReadHelper.extractValue(Values.doubleValue(42.0D), 0.0));
        assertTrue(Double.isNaN(ReadHelper.extractValue(Values.floatValue(Float.NaN), 0.0)));
        assertTrue(Double.isNaN(ReadHelper.extractValue(Values.doubleValue(Double.NaN), 0.0)));
    }

    @Test
    void extractValueReturnsDefaultWhenValueDoesNotExistOrIsNaN() {
        assertEquals(42.0, ReadHelper.extractValue(Values.NO_VALUE, 42.0));
    }

    @Test
    void extractValueReadsDatesAsEpochMillis() {
        LocalDateTime ldt = LocalDateTime.ofEpochSecond(1337421337, 420_000_000, ZoneOffset.UTC);
        assertEquals(1337421337420.0, ReadHelper.extractValue(Values.temporalValue(ldt), 42.0));

        OffsetDateTime odt = OffsetDateTime.of(ldt, ZoneOffset.UTC);
        assertEquals(1337421337420.0, ReadHelper.extractValue(Values.temporalValue(odt), 42.0));

        ZonedDateTime zdt = ZonedDateTime.of(ldt, ZoneOffset.UTC);
        assertEquals(1337421337420.0, ReadHelper.extractValue(Values.temporalValue(zdt), 42.0));
    }

    @Test
    void extractValueFailsForNonNumericTypes() {
        IllegalArgumentException exception;

        exception = assertThrows(IllegalArgumentException.class, () -> {
            ReadHelper.extractValue(Values.booleanValue(true), 42.0);
        });
        assertEquals(
                "Unsupported type [BOOLEAN] of value Boolean('true'). Please use a numeric property.",
                exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> {
            ReadHelper.extractValue(Values.stringValue("42"), 42.0);
        });
        assertEquals(
                "Unsupported type [TEXT] of value String(\"42\"). Please use a numeric property.",
                exception.getMessage());
    }
}
