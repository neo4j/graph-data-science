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

import org.neo4j.gds.api.ValueConversion;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.values.FloatingPointValue;
import org.neo4j.gds.values.GdsNoValue;
import org.neo4j.gds.values.GdsValue;
import org.neo4j.gds.values.IntegralValue;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class RelationshipPropertyExtractor {

    private RelationshipPropertyExtractor() {
        throw new UnsupportedOperationException("No instances");
    }

    public static double extractValue(GdsValue value, double defaultValue) {
        return extractValue(Aggregation.NONE, value, defaultValue);
    }

    public static double extractValue(Aggregation aggregation, GdsValue value, double defaultValue) {
        // slightly different logic than org.neo4j.values.storable.Values#coerceToDouble
        // b/c we want to fall back to the default value if the value is empty
        if (value instanceof FloatingPointValue) {
            double propertyValue = ((FloatingPointValue) value).doubleValue();
            return aggregation.normalizePropertyValue(propertyValue);
        }
        if (value instanceof IntegralValue) {
            double propertyValue = ValueConversion.exactLongToDouble(((IntegralValue) value).longValue());
            return aggregation.normalizePropertyValue(propertyValue);
        }
        if (GdsNoValue.NO_VALUE.equals(value)) {
            return aggregation.emptyValue(defaultValue);
        }

        // TODO: We used to do be lenient and parse strings/booleans into doubles.
        //       Do we want to do so or is failing on non numeric properties ok?
        throw new IllegalArgumentException(formatWithLocale(
            "Unsupported type [%s] of value %s. Please use a numeric property.",
            value.type(),
            value
        ));
    }
}
