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
package org.neo4j.gds.core.io.schema;

import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;

public class NodeSchemaBuilderVisitor extends NodeSchemaVisitor {

    private final NodeSchema nodeSchema;

    public NodeSchemaBuilderVisitor() {
        nodeSchema = NodeSchema.empty();
    }

    @Override
    protected void export() {
        var entry = nodeSchema.getOrCreateLabel(nodeLabel());

        if (key() != null) {
            entry.addProperty(
                key(),
                PropertySchema.of(key(), valueType(), defaultValue(), state())
            );
        }
    }

    public NodeSchema schema() {
        return nodeSchema;
    }
}
