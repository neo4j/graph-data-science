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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.core.write.RelationshipPropertiesExporter;

import java.util.List;
import java.util.Optional;
import java.util.function.LongUnaryOperator;

public class ResultStoreRelationshipPropertiesExporter implements RelationshipPropertiesExporter {

    private final GraphStore graphStore;
    private final ResultStore resultStore;
    private final LongUnaryOperator toOriginalId;

    ResultStoreRelationshipPropertiesExporter(
        GraphStore graphStore,
        ResultStore resultStore,
        LongUnaryOperator toOriginalId
    ) {
        this.graphStore = graphStore;
        this.resultStore = resultStore;
        this.toOriginalId = toOriginalId;
    }

    @Override
    public void write(String relationshipType, List<String> propertyKeys) {
        if (propertyKeys.isEmpty()) {
            var graph = graphStore.getGraph(RelationshipType.of(relationshipType));
            resultStore.addRelationship(relationshipType, graph, graph::toOriginalNodeId);
        } else if (propertyKeys.size() == 1) {
            var propertyKey = propertyKeys.get(0);
            var graph = graphStore.getGraph(RelationshipType.of(relationshipType), Optional.of(propertyKey));
            resultStore.addRelationship(relationshipType, propertyKey, graph, graph::toOriginalNodeId);
        } else {
            var relationshipIterator = graphStore.getCompositeRelationshipIterator(RelationshipType.of(relationshipType), propertyKeys);
            resultStore.addRelationshipIterator(relationshipType, propertyKeys, relationshipIterator, toOriginalId);
        }
    }
}
