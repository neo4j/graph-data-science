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

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.function.LongConsumer;

public final class ReadHelper {

    private ReadHelper() {
        throw new UnsupportedOperationException("No instances");
    }

    public static double readProperty(PropertyCursor pc, int propertyId, double defaultValue) {
        while (pc.next()) {
            if (pc.propertyKey() == propertyId) {
                Value value = pc.propertyValue();
                return extractValue(value, defaultValue);
            }
        }
        return defaultValue;
    }

    public static void readNodes(CursorFactory cursors, Read dataRead, int labelId, LongConsumer action) {
        if (labelId == Read.ANY_LABEL) {
            try (NodeCursor nodeCursor = cursors.allocateNodeCursor()) {
                dataRead.allNodesScan(nodeCursor);
                while (nodeCursor.next()) {
                    action.accept(nodeCursor.nodeReference());
                }
            }
        } else {
            try (NodeLabelIndexCursor nodeCursor = cursors.allocateNodeLabelIndexCursor()) {
                dataRead.nodeLabelScan(labelId, nodeCursor);
                while (nodeCursor.next()) {
                    action.accept(nodeCursor.nodeReference());
                }
            }
        }
    }

    public static double extractValue(Value value, double defaultValue) {
        // slightly different logic than org.neo4j.values.storable.Values#coerceToDouble
        // b/c we want to fallback to the default weight if the value is empty
        if (value instanceof NumberValue) {
            return ((NumberValue) value).doubleValue();
        }
        if (Values.NO_VALUE.eq(value)) {
            return defaultValue;
        }

        // TODO: We used to do be lenient and parse strings/booleans into doubles.
        //       Do we want to do so or is failing on non numeric properties ok?
        throw new IllegalArgumentException(String.format(
                "Unsupported type [%s] of value %s. Please use a numeric property.",
                value.valueGroup(),
                value));
    }
}
