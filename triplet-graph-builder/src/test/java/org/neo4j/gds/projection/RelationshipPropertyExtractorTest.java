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
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.values.primitive.PrimitiveValues;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipPropertyExtractorTest {

    @Test
    void extractValueReadsAnyNumericType() {
        Assertions.assertEquals(42.0, RelationshipPropertyExtractor.extractValue(PrimitiveValues.longValue((byte) 42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(PrimitiveValues.longValue((short) 42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(PrimitiveValues.longValue(42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(PrimitiveValues.longValue(42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(PrimitiveValues.floatingPointValue(42.0F), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(PrimitiveValues.floatingPointValue(42.0D), 0.0));
        assertTrue(Double.isNaN(RelationshipPropertyExtractor.extractValue(PrimitiveValues.floatingPointValue(Float.NaN), 0.0)));
        assertTrue(Double.isNaN(RelationshipPropertyExtractor.extractValue(PrimitiveValues.floatingPointValue(Double.NaN), 0.0)));
    }

    @ParameterizedTest
    @EnumSource(value = Aggregation.class, names = "COUNT", mode = EnumSource.Mode.EXCLUDE)
    void extractValueReadsAnyNumericTypeWithAggregationExceptCount(Aggregation aggregation) {
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.longValue((byte) 42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.longValue((short) 42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.longValue(42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.longValue(42), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.floatingPointValue(42.0F), 0.0));
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.floatingPointValue(42.0D), 0.0));
        assertTrue(Double.isNaN(RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.floatingPointValue(Float.NaN), 0.0)));
        assertTrue(Double.isNaN(RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.floatingPointValue(Double.NaN), 0.0)));
    }

    @ParameterizedTest
    @EnumSource(value = Aggregation.class, names = "COUNT", mode = EnumSource.Mode.INCLUDE)
    void extractValueReadsAnyNumericTypeWithCountAggregation(Aggregation aggregation) {
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.longValue((byte) 42), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.longValue((short) 42), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.longValue(42), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.longValue(42), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.floatingPointValue(42.0F), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.floatingPointValue(42.0D), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.floatingPointValue(Float.NaN), 0.0));
        assertEquals(1.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.floatingPointValue(Double.NaN), 0.0));
    }

    @Test
    void extractValueReturnsDefaultWhenValueDoesNotExist() {
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(PrimitiveValues.NO_VALUE, 42.0));
    }

    @ParameterizedTest
    @EnumSource(value = Aggregation.class, names = "COUNT", mode = EnumSource.Mode.EXCLUDE)
    void extractValueReturnsDefaultWhenValueDoesNotExistForAggregationsExceptCount(Aggregation aggregation) {
        assertEquals(42.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.NO_VALUE, 42.0));
    }

    @ParameterizedTest
    @EnumSource(value = Aggregation.class, names = "COUNT", mode = EnumSource.Mode.INCLUDE)
    void extractValueReturnsZeroWhenValueDoesNotExistForCountAggregation(Aggregation aggregation) {
        assertEquals(0.0, RelationshipPropertyExtractor.extractValue(aggregation, PrimitiveValues.NO_VALUE, 42.0));
    }
}
