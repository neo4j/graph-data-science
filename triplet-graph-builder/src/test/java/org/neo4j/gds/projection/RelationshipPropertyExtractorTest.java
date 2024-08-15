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
package org.neo4j.gds.projection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class RelationshipPropertyExtractorTest {

    @Test
    void extractValueReadsAnyNumericType() {
        Assertions.assertEquals(42.0, RelationshipPropertyExtractor.extractValue(Values.byteValue((byte) 42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(Values.shortValue((short) 42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(Values.intValue(42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(Values.longValue(42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(Values.floatValue(42.0F), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(Values.doubleValue(42.0D), 0.0));
        assertTrue(Double.isNaN(RelationshipPropertyExtractor.extractValue(Values.floatValue(Float.NaN), 0.0)));
        assertTrue(Double.isNaN(RelationshipPropertyExtractor.extractValue(Values.doubleValue(Double.NaN), 0.0)));
    }

    @ParameterizedTest
    @EnumSource(value = Aggregation.class, names = "COUNT", mode = EnumSource.Mode.EXCLUDE)
    void extractValueReadsAnyNumericTypeWithAggregationExceptCount(Aggregation aggregation) {
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.byteValue((byte) 42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.shortValue((short) 42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.intValue(42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.longValue(42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.floatValue(42.0F), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.doubleValue(42.0D), 0.0));
        assertTrue(Double.isNaN(RelationshipPropertyExtractor.extractValue(aggregation, Values.floatValue(Float.NaN), 0.0)));
        assertTrue(Double.isNaN(RelationshipPropertyExtractor.extractValue(aggregation, Values.doubleValue(Double.NaN), 0.0)));
    }

    @ParameterizedTest
    @EnumSource(value = Aggregation.class, names = "COUNT", mode = EnumSource.Mode.INCLUDE)
    void extractValueReadsAnyNumericTypeWithCountAggregation(Aggregation aggregation) {
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.byteValue((byte) 42), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.shortValue((short) 42), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.intValue(42), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.longValue(42), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.floatValue(42.0F), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.doubleValue(42.0D), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.floatValue(Float.NaN), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.doubleValue(Double.NaN), 0.0));
    }

    @Test
    void extractValueReturnsDefaultWhenValueDoesNotExist() {
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(Values.NO_VALUE, 42.0));
    }

    @ParameterizedTest
    @EnumSource(value = Aggregation.class, names = "COUNT", mode = EnumSource.Mode.EXCLUDE)
    void extractValueReturnsDefaultWhenValueDoesNotExistForAggregationsExceptCount(Aggregation aggregation) {
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.NO_VALUE, 42.0));
    }

    @ParameterizedTest
    @EnumSource(value = Aggregation.class, names = "COUNT", mode = EnumSource.Mode.INCLUDE)
    void extractValueReturnsZeroWhenValueDoesNotExistForCountAggregation(Aggregation aggregation) {
        assertEquals(0.0, RelationshipPropertyExtractor.extractValue(aggregation, Values.NO_VALUE, 42.0));
    }

    @ParameterizedTest
    @MethodSource("invalidProperties")
    void extractValueFailsForNonNumericTypes(Value value, String typePart, String valuePart) {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RelationshipPropertyExtractor.extractValue(value, 42.0)
        );
        String expectedErrorMessage = formatWithLocale(
                "Unsupported type [%s] of value %s. Please use a numeric property.",
                typePart,
                valuePart
        );
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("invalidPropertyAndAnyAggregation")
    void extractValueFailsForNonNumericTypesAndAggregation(Value value, String typePart, String valuePart, Aggregation aggregation) {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> RelationshipPropertyExtractor.extractValue(aggregation, value, 42.0)
        );
        String expectedErrorMessage = formatWithLocale(
                "Unsupported type [%s] of value %s. Please use a numeric property.",
                typePart,
                valuePart
        );
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    static Stream<Arguments> invalidProperties() {
        return Stream.of(
                arguments(Values.booleanValue(true), "BOOLEAN", "Boolean('true')"),
                arguments(Values.stringValue("42"), "TEXT", "String(\"42\")")
        );
    }

    static Stream<Arguments> invalidPropertyAndAnyAggregation() {
        return crossArguments(RelationshipPropertyExtractorTest::invalidProperties, () -> Arrays.stream(Aggregation.values()).map(Arguments::of));
    }
}
