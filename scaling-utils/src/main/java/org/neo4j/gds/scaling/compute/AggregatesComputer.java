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
package org.neo4j.gds.scaling.compute;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public abstract class AggregatesComputer implements Runnable {
    private final Partition partition;
    private final NodePropertyValues properties;
    private final ProgressTracker progressTracker;
    private long nodeCountOmittingMissingProperties;

    AggregatesComputer(Partition partition, NodePropertyValues property, ProgressTracker progressTracker) {
        this.partition = partition;
        this.properties = property;
        this.progressTracker = progressTracker;
        this.nodeCountOmittingMissingProperties = 0L;
    }

    abstract void compute(double propertyValue);

    @Override
    public void run() {
        long end = partition.startNode() + partition.nodeCount();
        for (long nodeId = partition.startNode(); nodeId < end; nodeId++) {
            var propertyValue = properties.doubleValue(nodeId);
            if (!Double.isNaN(propertyValue)) {
                ++nodeCountOmittingMissingProperties;
                compute(propertyValue);
            }
        }
        progressTracker.onProgress(partition.nodeCount());
    }

    long nodeCountOmittingMissingValues() {
        return nodeCountOmittingMissingProperties;
    }
}
