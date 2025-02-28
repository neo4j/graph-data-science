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

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.concurrent.atomic.AtomicInteger;

public class KdTreeBuilder {

    private final long nodeCount;
    private final NodePropertyValues nodePropertyValues;
    private final int concurrency;
    private final long leafSize;
    private final ProgressTracker progressTracker;
    private final Distances distances;

    public KdTreeBuilder(
        long nodeCount,
        NodePropertyValues nodePropertyValues,
        int concurrency,
        long leafSize,
        Distances distances,
        ProgressTracker progressTracker
    ) {
        this.nodeCount = nodeCount;
        this.nodePropertyValues = nodePropertyValues;
        this. concurrency = concurrency;
        this.leafSize = leafSize;
        this.progressTracker = progressTracker;

        this.distances = distances;
    }

    public KdTree build(){

        var ids = HugeLongArray.newArray(nodeCount);
        ids.setAll(  v-> v);

        var kdNodeSupport = KDNodeSupportFactory.create(nodePropertyValues,ids,nodePropertyValues.dimension().orElseThrow());
        AtomicInteger nodeIndex = new AtomicInteger(0);

        var builderTask = new KdTreeNodeBuilderTask(
            kdNodeSupport,
            nodePropertyValues,
            ids,
            0,
            nodeCount,
            leafSize,
            nodeIndex,
            null,
            false,
            progressTracker
        );

        progressTracker.beginSubTask();
        builderTask.run();
        var root = builderTask.kdNode();
        progressTracker.endSubTask();
        return new KdTree(ids, distances, root, nodeIndex.get());
    }

}
