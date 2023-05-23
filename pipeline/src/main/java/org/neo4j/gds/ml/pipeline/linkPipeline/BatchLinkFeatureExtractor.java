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
package org.neo4j.gds.ml.pipeline.linkPipeline;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

class BatchLinkFeatureExtractor implements Runnable {
    final LinkFeatureExtractor extractor;
    final DegreePartition partition;
    final long relationshipOffset;
    final Graph graph;
    final HugeObjectArray<double[]> linkFeatures;
    final ProgressTracker progressTracker;

    BatchLinkFeatureExtractor(
        LinkFeatureExtractor extractor,
        DegreePartition partition,
        Graph graph,
        long relationshipOffset,
        HugeObjectArray<double[]> linkFeatures,
        ProgressTracker progressTracker
    ) {
        this.extractor = extractor;
        this.partition = partition;
        this.relationshipOffset = relationshipOffset;
        this.graph = graph;
        this.linkFeatures = linkFeatures;
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        var currentRelationshipOffset = new MutableLong(relationshipOffset);

        partition.consume(nodeId -> {
            graph.forEachRelationship(nodeId, ((sourceNodeId, targetNodeId) -> {
                var features = extractor.extractFeatures(sourceNodeId, targetNodeId);
                linkFeatures.set(currentRelationshipOffset.getAndIncrement(), features);
                return true;
            }));
        });

        progressTracker.logSteps(partition.relationshipCount());
    }
}

