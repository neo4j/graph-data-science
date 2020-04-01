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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.compat.StatementConstantsProxy;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.Arrays;

public final class ReadHelper {

    private ReadHelper() {
        throw new UnsupportedOperationException("No instances");
    }

    public static void readProperties(
        PropertyCursor pc,
        int[] propertyIds,
        double[] defaultValues,
        Aggregation[] aggregations,
        double[] properties
    ) {
        Arrays.setAll(properties, indexOfPropertyId -> {
            double defaultValue = defaultValues[indexOfPropertyId];
            Aggregation aggregation = aggregations[indexOfPropertyId];
            if (propertyIds[indexOfPropertyId] == StatementConstantsProxy.NO_SUCH_PROPERTY_KEY) {
                // we are in `count(*)` mode, so we don't expect any loads on the property
                // and need to return a valid count value, since we are just counting rows
                return aggregation.normalizePropertyValue(defaultValue);
            } else {
                return aggregation.emptyValue(defaultValue);
            }
        });
        while (pc.next()) {
            // TODO: We used ArrayUtil#linearSearchIndex before which looks at four array positions in one loop iteration.
            //       We could do the same here and benchmark if it affects performance.
            for (int indexOfPropertyId = 0; indexOfPropertyId < propertyIds.length; indexOfPropertyId++) {
                if (propertyIds[indexOfPropertyId] == pc.propertyKey()) {
                    Aggregation aggregation = aggregations[indexOfPropertyId];
                    Value value = pc.propertyValue();
                    double defaultValue = defaultValues[indexOfPropertyId];
                    double propertyValue = extractValue(aggregation, value, defaultValue);
                    properties[indexOfPropertyId] = propertyValue;
                }
            }
        }
    }

    public static double extractValue(Value value, double defaultValue) {
        return extractValue(Aggregation.NONE, value, defaultValue);
    }

    public static double extractValue(Aggregation aggregation, Value value, double defaultValue) {
        // slightly different logic than org.neo4j.values.storable.Values#coerceToDouble
        // b/c we want to fallback to the default value if the value is empty
        if (value instanceof NumberValue) {
            double propertyValue = ((NumberValue) value).doubleValue();
            return aggregation.normalizePropertyValue(propertyValue);
        }
        if (Values.NO_VALUE.equals(value)) {
            return aggregation.emptyValue(defaultValue);
        }

        // TODO: We used to do be lenient and parse strings/booleans into doubles.
        //       Do we want to do so or is failing on non numeric properties ok?
        throw new IllegalArgumentException(String.format(
            "Unsupported type [%s] of value %s. Please use a numeric property.",
            value.valueGroup(),
            value
        ));
    }
}
