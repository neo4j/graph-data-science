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
package org.neo4j.gds.core.io.file;

import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.values.GdsValue;
import org.neo4j.gds.values.primitive.PrimitiveValues;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class GraphStoreNodeVisitor extends NodeVisitor {

    private final NodesBuilder nodesBuilder;
    private final Map<String, Function<Object, GdsValue>> propertyCreators;

    public GraphStoreNodeVisitor(NodeSchema nodeSchema, NodesBuilder nodesBuilder) {
        super(nodeSchema);
        this.nodesBuilder = nodesBuilder;

        propertyCreators = initPropertyCreators(nodeSchema);
    }

    private static Map<String, Function<Object, GdsValue>> initPropertyCreators(NodeSchema nodeSchema) {
        Map<String, Function<Object, GdsValue>> propertyCreators = new HashMap<>();
        for (var labelSchema : nodeSchema.entries().entrySet()) {
            for (PropertySchema propertySchema : labelSchema.getValue()) {
                propertyCreators.putIfAbsent(propertySchema.key(), PrimitiveValues.valueCreator(propertySchema.valueType()));
            }
        }
        return propertyCreators;
    }

    @Override
    protected void exportElement() {
        Map<String, GdsValue> props = new HashMap<>();
        forEachProperty((key, value) -> {
            props.put(key, propertyCreators.get(key).apply(value));
        });
        var nodeLabels = NodeLabelTokens.of(labels());
        nodesBuilder.addNode(id(), props, nodeLabels);
    }

    public static final class Builder extends NodeVisitor.Builder<Builder, GraphStoreNodeVisitor> {

        private NodesBuilder nodesBuilder;

        Builder withNodesBuilder(NodesBuilder nodesBuilder) {
            this.nodesBuilder = nodesBuilder;
            return this;
        }

        @Override
        Builder me() {
            return this;
        }

        @Override
        public GraphStoreNodeVisitor build() {
            return new GraphStoreNodeVisitor(nodeSchema, nodesBuilder);
        }
    }
}
