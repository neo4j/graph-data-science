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
package org.neo4j.gds.core.write.resultstore;

import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;

import java.util.Collection;
import java.util.List;

public class ResultStoreNodePropertyExporter implements NodePropertyExporter {

    private final ResultStore resultStore;
    private long writtenProperties;

    ResultStoreNodePropertyExporter(ResultStore resultStore) {
        this.resultStore = resultStore;
    }

    @Override
    public void write(String property, NodePropertyValues properties) {
        write(ImmutableNodeProperty.of(property, properties));
    }

    @Override
    public void write(NodeProperty nodeProperty) {
        write(List.of(nodeProperty));
    }

    @Override
    public void write(Collection<NodeProperty> nodeProperties) {
        nodeProperties.forEach(nodeProperty -> {
            var propertyValues = nodeProperty.properties();
            resultStore.addNodePropertyValues(
                nodeProperty.propertyKey(),
                propertyValues
            );
            writtenProperties += propertyValues.nodeCount();
        });
    }

    @Override
    public long propertiesWritten() {
        return writtenProperties;
    }
}
