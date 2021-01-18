/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.context.MasterComputeContext;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PUBLIC, depluralize = true, deepImmutablesDetection = true)
public final class Pregel<CONFIG extends PregelConfig> {

    private final CONFIG config;

    private final PregelComputation<CONFIG> computation;

    private final Graph graph;

    private final CompositeNodeValue nodeValues;

    private final Messenger<?> messenger;

    private final int concurrency;
    private final ExecutorService executor;
    private final AllocationTracker tracker;

    public static <CONFIG extends PregelConfig> Pregel<CONFIG> create(
        Graph graph,
        CONFIG config,
        PregelComputation<CONFIG> computation,
        ExecutorService executor,
        AllocationTracker tracker
    ) {
        // This prevents users from disabling concurrency
        // validation in custom PregelConfig implementations.
        // Creating a copy of the user config triggers the
        // concurrency validations.
        ImmutablePregelConfig.copyOf(config);

        return new Pregel<>(
            graph,
            config,
            computation,
            CompositeNodeValue.of(computation.schema(), graph.nodeCount(), config.concurrency(), tracker),
            executor,
            tracker
        );
    }

    public static MemoryEstimation memoryEstimation(PregelSchema pregelSchema, boolean isQueueBased) {
        var estimationBuilder = MemoryEstimations.builder(Pregel.class)
            .perNode("message bits", MemoryUsage::sizeOfHugeAtomicBitset)
            .perNode("previous message bits", MemoryUsage::sizeOfHugeAtomicBitset)
            .perNode("vote bits", MemoryUsage::sizeOfHugeAtomicBitset)
            .perThread("compute steps", MemoryEstimations.builder(ComputeStep.class).build())
            .add("composite node value", CompositeNodeValue.memoryEstimation(pregelSchema));

        if (isQueueBased) {
            estimationBuilder.add("message queues", QueueMessenger.memoryEstimation());
        } else {
            estimationBuilder.add("message arrays", ReducingMessenger.memoryEstimation());
        }

        return estimationBuilder.build();
    }

    private Pregel(
        final Graph graph,
        final CONFIG config,
        final PregelComputation<CONFIG> computation,
        final CompositeNodeValue initialNodeValues,
        final ExecutorService executor,
        final AllocationTracker tracker
    ) {
        this.graph = graph;
        this.config = config;
        this.computation = computation;
        this.nodeValues = initialNodeValues;
        this.concurrency = config.concurrency();
        this.executor = executor;
        this.tracker = tracker;

        var reducer = computation.reducer();

        this.messenger = reducer.isPresent()
            ? new ReducingMessenger(graph, config, reducer.get(), tracker)
            : new QueueMessenger(graph, config, tracker);
    }

    public PregelResult run() {
        boolean didConverge = false;
        // Tracks if a node received messages in the current iteration
        HugeAtomicBitSet messageBits = HugeAtomicBitSet.create(graph.nodeCount(), tracker);
        // Tracks if a node received messages in the previous iteration
        HugeAtomicBitSet prevMessageBits = HugeAtomicBitSet.create(graph.nodeCount(), tracker);
        // Tracks if a node voted to halt in the previous iteration
        HugeAtomicBitSet voteBits = HugeAtomicBitSet.create(graph.nodeCount(), tracker);

        var computeSteps = createComputeSteps(voteBits);

        int iterations;
        for (iterations = 0; iterations < config.maxIterations(); iterations++) {
            if (iterations > 0) {
                messageBits.clear();
            }

            // Init compute steps with the updated state
            for (var computeStep : computeSteps) {
                computeStep.init(iterations, messageBits, prevMessageBits);
            }

            // Init messenger with the updated state
            messenger.initIteration(iterations, prevMessageBits);

            // Run the computation
            runComputeSteps(computeSteps);
            runMasterComputeStep(iterations);

            // No messages have been sent and all nodes voted to halt
            if (messageBits.isEmpty() && voteBits.allSet()) {
                didConverge = true;
                break;
            }

            // Swap message bits for next iteration
            var tmp = messageBits;
            messageBits = prevMessageBits;
            prevMessageBits = tmp;
        }

        return ImmutablePregelResult.builder()
            .nodeValues(nodeValues)
            .didConverge(didConverge)
            .ranIterations(iterations)
            .build();
    }

    public void release() {
        messenger.release();
    }

    private List<ComputeStep<CONFIG, ?>> createComputeSteps(HugeAtomicBitSet voteBits) {
        List<Partition> partitions = PartitionUtils.rangePartition(concurrency, graph.nodeCount());

        List<ComputeStep<CONFIG, ?>> computeSteps = new ArrayList<>(concurrency);

        for (Partition partition : partitions) {
            computeSteps.add(new ComputeStep<>(
                graph,
                computation,
                config,
                0,
                partition,
                nodeValues,
                messenger,
                voteBits,
                graph
            ));
        }
        return computeSteps;
    }

    private void runComputeSteps(Collection<ComputeStep<CONFIG, ?>> computeSteps) {
        ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
    }

    private void runMasterComputeStep(int iteration) {
        var context = new MasterComputeContext<>(config, graph, iteration, nodeValues);
        computation.masterCompute(context);
    }

    public static final class CompositeNodeValue {

        private final PregelSchema pregelSchema;
        private final Map<String, Object> properties;

        static CompositeNodeValue of(
            PregelSchema pregelSchema,
            long nodeCount,
            int concurrency,
            AllocationTracker tracker
        ) {
            Map<String, Object> properties = new HashMap<>();

            pregelSchema.elements().forEach(element -> {
                switch (element.propertyType()) {
                    case DOUBLE:
                        var doubleNodeValues = HugeDoubleArray.newArray(nodeCount, tracker);
                        ParallelUtil.parallelStreamConsume(
                            LongStream.range(0, nodeCount),
                            concurrency,
                            nodeIds -> nodeIds.forEach(nodeId -> doubleNodeValues.set(
                                nodeId,
                                DefaultValue.DOUBLE_DEFAULT_FALLBACK
                            ))
                        );
                        properties.put(element.propertyKey(), doubleNodeValues);
                        break;
                    case LONG:
                        var longNodeValues = HugeLongArray.newArray(nodeCount, tracker);
                        ParallelUtil.parallelStreamConsume(
                            LongStream.range(0, nodeCount),
                            concurrency,
                            nodeIds -> nodeIds.forEach(nodeId -> longNodeValues.set(
                                nodeId,
                                DefaultValue.LONG_DEFAULT_FALLBACK
                            ))
                        );
                        properties.put(element.propertyKey(), longNodeValues);
                        break;
                    case LONG_ARRAY:
                        properties.put(
                            element.propertyKey(),
                            HugeObjectArray.newArray(long[].class, nodeCount, tracker)
                        );
                        break;
                    case DOUBLE_ARRAY:
                        properties.put(
                            element.propertyKey(),
                            HugeObjectArray.newArray(double[].class, nodeCount, tracker)
                        );
                        break;
                    default:
                        throw new IllegalArgumentException(formatWithLocale(
                            "Unsupported value type: %s",
                            element.propertyType()
                        ));
                }
            });

            return new CompositeNodeValue(pregelSchema, properties);
        }

        static MemoryEstimation memoryEstimation(PregelSchema pregelSchema) {
            return MemoryEstimations.setup("", (dimensions, concurrency) -> {
                var builder = MemoryEstimations.builder();

                pregelSchema.elements().forEach(element -> {
                    var entry = formatWithLocale("%s (%s)", element.propertyKey(), element.propertyType());

                    switch (element.propertyType()) {
                        case LONG:
                            builder.fixed(entry, HugeLongArray.memoryEstimation(dimensions.nodeCount()));
                            break;
                        case DOUBLE:
                            builder.fixed(entry, HugeDoubleArray.memoryEstimation(dimensions.nodeCount()));
                            break;
                        case LONG_ARRAY:
                            builder.add(entry, MemoryEstimations.builder()
                                .fixed(
                                    HugeObjectArray.class.getSimpleName(),
                                    MemoryUsage.sizeOfInstance(HugeObjectArray.class)
                                )
                                .perNode("long[10]", nodeCount -> nodeCount * MemoryUsage.sizeOfLongArray(10))
                                .build());
                            break;
                        case DOUBLE_ARRAY:
                            builder.add(entry, MemoryEstimations.builder()
                                .fixed(
                                    HugeObjectArray.class.getSimpleName(),
                                    MemoryUsage.sizeOfInstance(HugeObjectArray.class)
                                )
                                .perNode("double[10]", nodeCount -> nodeCount * MemoryUsage.sizeOfDoubleArray(10))
                                .build());
                            break;
                        default:
                            builder.add(entry, MemoryEstimations.empty());
                    }
                });

                return builder.build();
            });
        }

        private CompositeNodeValue(
            PregelSchema pregelSchema,
            Map<String, Object> properties
        ) {
            this.pregelSchema = pregelSchema;
            this.properties = properties;
        }

        public PregelSchema schema() {
            return pregelSchema;
        }

        public HugeDoubleArray doubleProperties(String propertyKey) {
            return checkProperty(propertyKey, HugeDoubleArray.class);
        }

        public HugeLongArray longProperties(String propertyKey) {
            return checkProperty(propertyKey, HugeLongArray.class);
        }

        public HugeObjectArray<long[]> longArrayProperties(String propertyKey) {
            return checkProperty(propertyKey, HugeObjectArray.class);
        }

        public HugeObjectArray<double[]> doubleArrayProperties(String propertyKey) {
            return checkProperty(propertyKey, HugeObjectArray.class);
        }

        public double doubleValue(String key, long nodeId) {
            return doubleProperties(key).get(nodeId);
        }

        public long longValue(String key, long nodeId) {
            return longProperties(key).get(nodeId);
        }

        public long[] longArrayValue(String key, long nodeId) {
            HugeObjectArray<long[]> arrayProperties = longArrayProperties(key);
            return arrayProperties.get(nodeId);
        }

        public double[] doubleArrayValue(String key, long nodeId) {
            HugeObjectArray<double[]> arrayProperties = doubleArrayProperties(key);
            return arrayProperties.get(nodeId);
        }

        public void set(String key, long nodeId, double value) {
            doubleProperties(key).set(nodeId, value);
        }

        public void set(String key, long nodeId, long value) {
            longProperties(key).set(nodeId, value);
        }

        public void set(String key, long nodeId, long[] value) {
            longArrayProperties(key).set(nodeId, value);
        }

        public void set(String key, long nodeId, double[] value) {
            doubleArrayProperties(key).set(nodeId, value);
        }

        @SuppressWarnings("unchecked")
        private <PROPERTY> PROPERTY checkProperty(String key, Class<? extends PROPERTY> propertyKlass) {
            var property = properties.get(key);
            if (property == null) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Property with key %s does not exist. Available properties are: %s",
                    key,
                    properties.keySet()
                ));
            }

            if (!(propertyKlass.isAssignableFrom(property.getClass()))) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Could not cast property %s of type %s into %s",
                    key,
                    property.getClass().getSimpleName(),
                    propertyKlass.getSimpleName()
                ));
            }

            return (PROPERTY) property;
        }
    }

    @ValueClass
    public interface PregelResult {

        CompositeNodeValue nodeValues();

        int ranIterations();

        boolean didConverge();
    }
}
