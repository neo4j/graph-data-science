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

import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

class GenerateFeaturesTask implements Runnable {
    private final Partition partition;
    private final HugeObjectArray<HugeAtomicBitSet> output;
    private final SplittableRandom rng;
    private final GenerateFeaturesConfig generateFeaturesConfig;
    private final ProgressTracker progressTracker;
    private long totalNumFeatures = 0;

    GenerateFeaturesTask(
        Partition partition,
        SplittableRandom rng,
        GenerateFeaturesConfig config,
        HugeObjectArray<HugeAtomicBitSet> output,
        ProgressTracker progressTracker
    ) {
        this.partition = partition;
        this.rng = rng;
        this.generateFeaturesConfig = config;
        this.output = output;
        this.progressTracker = progressTracker;
    }

    static HugeObjectArray<HugeAtomicBitSet> compute(
        Graph graph,
        List<Partition> partition,
        HashGNNConfig config,
        SplittableRandom rng,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        MutableLong totalNumFeaturesOutput
    ) {
        progressTracker.beginSubTask("Generate base node property features");

        var output = HugeObjectArray.newArray(HugeAtomicBitSet.class, graph.nodeCount());

        var tasks = partition.stream()
            .map(p -> new GenerateFeaturesTask(
                p,
                rng.split(),
                config.generateFeatures().get(),
                output,
                progressTracker
            ))
            .collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .run();

        totalNumFeaturesOutput.add(tasks.stream().mapToLong(GenerateFeaturesTask::totalNumFeatures).sum());

        progressTracker.endSubTask("Generate base node property features");

        return output;
    }

    @Override
    public void run() {
        int dimension = generateFeaturesConfig.dimension();
        int densityLevel = generateFeaturesConfig.densityLevel();

        partition.consume(nodeId -> {
            var generatedFeatures = HugeAtomicBitSet.create(dimension);

            var randomInts = rng.ints(densityLevel, 0, dimension);
            randomInts.forEach(generatedFeatures::set);

            totalNumFeatures += generatedFeatures.cardinality();

            output.set(nodeId, generatedFeatures);
        });

        progressTracker.logProgress(partition.nodeCount());
    }

    public long totalNumFeatures() {
        return totalNumFeatures;
    }
}
