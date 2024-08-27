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
package org.neo4j.gds.values;

import org.neo4j.gds.api.properties.nodes.BinaryArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FilteredNodePropertyValuesMarker;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;

public final class Neo4jNodePropertyValuesUtil {

    public static Neo4jNodePropertyValues of(NodePropertyValues internal) {
        if (internal instanceof BinaryArrayNodePropertyValues asBinaryArrayNodePropertyValues) {
            return new Neo4jBinaryArrayNodePropertyValues(asBinaryArrayNodePropertyValues);
        }
        return switch(internal.valueType()) {
            case DOUBLE -> new Neo4jDoubleNodePropertyValues((DoubleNodePropertyValues) internal);
            case LONG -> new Neo4jLongNodePropertyValues((LongNodePropertyValues) internal, internal instanceof FilteredNodePropertyValuesMarker);
            case FLOAT_ARRAY -> new Neo4jFloatArrayNodePropertyValues((FloatArrayNodePropertyValues) internal);
            case DOUBLE_ARRAY -> new Neo4jDoubleArrayNodePropertyValues((DoubleArrayNodePropertyValues) internal);
            case LONG_ARRAY -> new Neo4jLongArrayNodePropertyValues((LongArrayNodePropertyValues) internal);
//            case STRING -> null;
//            case UNTYPED_ARRAY -> null;
//            case UNKNOWN -> null;
            default -> throw new IllegalArgumentException("Exporting values of type " + internal.valueType().csvName() + " is not supported.");
        };
    }

    private Neo4jNodePropertyValuesUtil() {}
}
