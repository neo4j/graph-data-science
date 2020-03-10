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
package org.neo4j.graphalgo.nodesim;

import org.neo4j.graphalgo.api.FilterGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;

public class TopKGraph extends FilterGraph {

    private final TopKMap topKMap;

    TopKGraph(Graph graph, TopKMap topKMap) {
        super(graph);
        this.topKMap = topKMap;
    }

    public Graph baseGraph() {
        return graph;
    }

    @Override
    public int degree(long nodeId) {
        TopKMap.TopKList topKList = topKMap.get(nodeId);
        return topKList != null ? topKList.size() : 0;
    }

    @Override
    public long relationshipCount() {
        return topKMap.similarityPairCount();
    }

    @Override
    public void forEachRelationship(long node1, RelationshipConsumer consumer) {
        TopKMap.TopKList topKList = topKMap.get(node1);
        if (topKList != null) {
            topKList.forEach((node2, similarity) -> consumer.accept(node1, node2));
        }
    }

    @Override
    public void forEachRelationship(long node1, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        TopKMap.TopKList topKList = topKMap.get(node1);
        if (topKList != null) {
            topKList.forEach((node2, similarity) -> consumer.accept(node1, node2, similarity));
        }
   }
}
