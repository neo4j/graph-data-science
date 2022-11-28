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
package org.neo4j.gds.embeddings.hashgnn;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.features.FeatureConsumer;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.features.FeatureExtractor;

import java.util.List;
import java.util.stream.Collectors;

class RawFeaturesTask implements Runnable {
    private final Partition partition;
    private final List<FeatureExtractor> featureExtractors;
    private final int inputDimension;
    private final HugeObjectArray<HugeAtomicBitSet> features;
    private final ProgressTracker progressTracker;
    private long totalFeatureCount = 0;

    RawFeaturesTask(
        Partition partition,
        List<FeatureExtractor> featureExtractors,
        int inputDimension,
        HugeObjectArray<HugeAtomicBitSet> features,
        ProgressTracker progressTracker
    ) {
        this.partition = partition;
        this.featureExtractors = featureExtractors;
        this.inputDimension = inputDimension;
        this.features = features;
        this.progressTracker = progressTracker;
    }

    static HugeObjectArray<HugeAtomicBitSet> compute(
        HashGNNConfig config,
        ProgressTracker progressTracker,
        Graph graph,
        List<Partition> partitions,
        TerminationFlag terminationFlag,
        MutableLong totalFeatureCountOutput
    ) {
        progressTracker.beginSubTask("Extract raw node property features");

        var featureExtractors = FeatureExtraction.propertyExtractors(
            graph,
            config.featureProperties()
        );
        int inputDimension = FeatureExtraction.featureCount(featureExtractors);

        var features = HugeObjectArray.newArray(HugeAtomicBitSet.class, graph.nodeCount());

        var tasks = partitions.stream()
            .map(p -> new RawFeaturesTask(
                p,
                featureExtractors,
                inputDimension,
                features,
                progressTracker
            ))
            .collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .run();

        totalFeatureCountOutput.add(tasks.stream().mapToLong(RawFeaturesTask::totalFeatureCount).sum());

        progressTracker.endSubTask("Extract raw node property features");

        return features;
    }

    @Override
    public void run() {
        partition.consume(nodeId -> {
            var nodeFeatures = HugeAtomicBitSet.create(inputDimension);
            FeatureExtraction.extract(nodeId, -1, featureExtractors, new FeatureConsumer() {
                @Override
                public void acceptScalar(long nodeOffset, int offset, double value) {
                    if (value != 0.0) {
                        nodeFeatures.set(offset);
                    }
                }

                @Override
                public void acceptArray(long nodeOffset, int offset, double[] values) {
                    for (int inputFeatureOffset = 0; inputFeatureOffset < values.length; inputFeatureOffset++) {
                        if (values[inputFeatureOffset] != 0.0) {
                            nodeFeatures.set(offset + inputFeatureOffset);
                        }
                    }
                }
            });
            totalFeatureCount += nodeFeatures.cardinality();
            features.set(nodeId, nodeFeatures);
        });

        progressTracker.logProgress(partition.nodeCount());
    }

    public long totalFeatureCount() {
        return totalFeatureCount;
    }
}
