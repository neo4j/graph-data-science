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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphAdapter;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.relationships.RelationshipConsumer;
import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.ImmutableMutableGraphSchema;
import org.neo4j.gds.api.schema.ImmutableRelationshipPropertySchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchemaEntry;
import org.neo4j.gds.numbers.Aggregation;

import java.util.Arrays;
import java.util.Map;

public class SpanningGraph extends GraphAdapter {

    private final SpanningTree spanningTree;

    public SpanningGraph(Graph graph, SpanningTree spanningTree) {
        super(graph);
        this.spanningTree = spanningTree;
    }

    @Override
    public int degree(long nodeId) {
        if (spanningTree.parent.get(nodeId) < 0) {
            return Math.toIntExact(Arrays.stream(spanningTree.parent.toArray()).filter(i -> i < 0).count());
        } else {
            return 1;
        }
    }

    @Override
    public long relationshipCount() {
        // not counting root -> root as a rel
        return spanningTree.effectiveNodeCount() - 1;
    }

    @Override
    public boolean hasRelationshipProperty() {
        return true;
    }

    @Override
    public GraphSchema schema() {
        var relationshipSchema = new MutableRelationshipSchema(
            Map.of(
                RelationshipType.ALL_RELATIONSHIPS,
                new MutableRelationshipSchemaEntry(
                    RelationshipType.ALL_RELATIONSHIPS,
                    Direction.DIRECTED,
                    Map.of(
                        "cost",
                        ImmutableRelationshipPropertySchema.of(
                            "cost",
                            ValueType.DOUBLE,
                            DefaultValue.DEFAULT,
                            PropertyState.TRANSIENT,
                            Aggregation.NONE
                        )
                    )
                )
            )
        );

        return ImmutableMutableGraphSchema.builder()
            .from(graph.schema())
            .relationshipSchema(relationshipSchema)
            .build();
    }

    @Override
    public boolean isMultiGraph() {
        return false;
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        forEachRelationship(
            nodeId,
            0.0,
            (sourceNodeId, targetNodeId, property) -> consumer.accept(sourceNodeId, targetNodeId)
        );
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        long parent = spanningTree.parent.get(nodeId);
        if (parent >=0) {
            consumer.accept(parent, nodeId, spanningTree.costToParent(nodeId));
        }
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        return spanningTree.parent.get(sourceNodeId) != -1 || spanningTree.parent.get(targetNodeId) != -1;
    }

    @Override
    public Graph concurrentCopy() {
        return this;
    }
}
