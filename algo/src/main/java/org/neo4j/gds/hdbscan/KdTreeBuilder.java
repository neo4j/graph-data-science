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
package org.neo4j.gds.hdbscan;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.concurrent.atomic.AtomicInteger;

public class KdTreeBuilder {

    private final  IdMap nodes;
    private final NodePropertyValues nodePropertyValues;
    private final int concurrency;
    private final long leafSize;

    public KdTreeBuilder(IdMap nodes, NodePropertyValues nodePropertyValues,int concurrency,long leafSize) {
        this.nodes = nodes;
        this.nodePropertyValues = nodePropertyValues;
        this. concurrency = concurrency;
        this.leafSize = leafSize;
    }

    public KdTree build(){

        var ids = HugeLongArray.newArray(nodes.nodeCount());
        ids.setAll(  v-> v);
        AtomicInteger nodeIndex = new AtomicInteger(0);
        var builderTask = new KdTreeNodeBuilderTask(ids,nodePropertyValues,0,nodePropertyValues.nodeCount(),leafSize,false,null,
            nodeIndex
        );
        builderTask.run();
        var root = builderTask.kdNode();

        return  new KdTree(ids, nodePropertyValues, root, nodeIndex.get());
    }

}
