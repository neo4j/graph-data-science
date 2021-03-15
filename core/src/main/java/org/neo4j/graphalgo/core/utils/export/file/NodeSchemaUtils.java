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
package org.neo4j.graphalgo.core.utils.export.file;

import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.NodeSchema;

import static org.neo4j.graphalgo.core.utils.export.file.NodeVisitor.NEO_ID_KEY;

public final class NodeSchemaUtils {

    private NodeSchemaUtils() {}

    public static NodeSchema computeNodeSchema(NodeSchema nodeSchema, boolean reverseIdMapping) {
        NodeSchema.Builder builder = NodeSchema.builder();
        if (reverseIdMapping) {
            nodeSchema.availableLabels().forEach(nodeLabel -> builder.addProperty(nodeLabel, NEO_ID_KEY, ValueType.LONG));
        }
        return nodeSchema.union(builder.build());
    }
}
