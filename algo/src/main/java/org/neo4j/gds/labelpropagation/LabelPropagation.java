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
package org.neo4j.gds.labelpropagation;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.NullPropertyMap.DoubleNullPropertyMap;
import org.neo4j.gds.core.loading.NullPropertyMap.LongNullPropertyMap;
import org.neo4j.gds.core.utils.LazyBatchCollection;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongCollections;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;

public class LabelPropagation extends Algorithm<LabelPropagationResult> {

    private final long nodeCount;
    private final NodePropertyValues nodePropertyValues;
    private final NodePropertyValues nodeWeights;
    private final LabelPropagationBaseConfig config;
    private final ExecutorService executor;

    private Graph graph;
    private HugeLongArray labels;
    private final long maxLabelId;
    private int batchSize;

    public LabelPropagation(
        Graph graph,
        LabelPropagationBaseConfig config,
        ExecutorService executor,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.config = config;
        this.executor = executor;
        this.batchSize = ParallelUtil.DEFAULT_BATCH_SIZE;

        NodePropertyValues seedProperty;
        String seedPropertyKey = config.seedProperty();
        if (seedPropertyKey != null && graph.availableNodeProperties().contains(seedPropertyKey)) {
            seedProperty = graph.nodeProperties(seedPropertyKey);
        } else {
            seedProperty = new LongNullPropertyMap(DefaultValue.LONG_DEFAULT_FALLBACK);
        }
        this.nodePropertyValues = seedProperty;

        NodePropertyValues nodeWeightProperty;
        String nodeWeightPropertyKey = config.nodeWeightProperty();
        if (nodeWeightPropertyKey != null && graph.availableNodeProperties().contains(nodeWeightPropertyKey)) {
            nodeWeightProperty = graph.nodeProperties(nodeWeightPropertyKey);
        } else {
            nodeWeightProperty = new DoubleNullPropertyMap(1.0);
        }
        this.nodeWeights = nodeWeightProperty;

        maxLabelId = seedProperty.getMaxLongPropertyValue().orElse(NO_SUCH_LABEL);
    }

    @Override
    public void release() {
        graph = null;
    }

    @Override
    public LabelPropagationResult compute() {
        if (config.maxIterations() <= 0L) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }

        progressTracker.beginSubTask();

        if (labels == null || labels.size() != nodeCount) {
            labels = HugeLongArray.newArray(nodeCount);
        }

        long ranIterations = 0L;
        boolean didConverge = false;

        List<StepRunner> stepRunners = stepRunners();

        progressTracker.beginSubTask();
        while (ranIterations < config.maxIterations()) {
            progressTracker.beginSubTask();
            RunWithConcurrency.builder()
                .concurrency(config.concurrency())
                .tasks(stepRunners)
                .waitTime(1L, MICROSECONDS)
                .terminationFlag(terminationFlag)
                .executor(executor)
                .run();
            ++ranIterations;
            didConverge = stepRunners.stream().allMatch(StepRunner::didConverge);
            progressTracker.endSubTask();
            if (didConverge) {
                break;
            }
        }
        progressTracker.endSubTask();

        stepRunners.forEach(StepRunner::release);
        progressTracker.endSubTask();

        return new LabelPropagationResult(labels, didConverge, ranIterations);
    }

    private List<StepRunner> stepRunners() {
        long nodeCount = graph.nodeCount();
        long batchSize = ParallelUtil.adjustedBatchSize(nodeCount, this.batchSize);

        Collection<PrimitiveLongIterable> nodeBatches = LazyBatchCollection.of(
            nodeCount,
            batchSize,
            (start, length) -> () -> PrimitiveLongCollections.range(start, start + length - 1L)
        );

        int threads = nodeBatches.size();
        List<StepRunner> tasks = new ArrayList<>(threads);
        for (PrimitiveLongIterable iter : nodeBatches) {
            InitStep initStep = new InitStep(
                graph,
                nodePropertyValues,
                nodeWeights,
                iter,
                labels,
                progressTracker,
                maxLabelId
            );
            StepRunner task = new StepRunner(initStep);
            tasks.add(task);
        }
        progressTracker.beginSubTask();
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .waitTime(1, MICROSECONDS)
            .terminationFlag(terminationFlag)
            .executor(executor)
            .run();
        progressTracker.endSubTask();
        return tasks;
    }

    void withBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
