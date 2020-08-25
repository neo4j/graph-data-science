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
package org.neo4j.graphalgo.beta.pregel;

import com.carrotsearch.hppc.BitSet;
import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.jctools.queues.MpscLinkedQueue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PUBLIC, depluralize = true, deepImmutablesDetection = true)
public final class Pregel<CONFIG extends PregelConfig> {

    // Marks the end of messages from the previous iteration in synchronous mode.
    private static final Double TERMINATION_SYMBOL = Double.NaN;

    private final CONFIG config;

    private final PregelComputation<CONFIG> computation;

    private final Graph graph;

    private final CompositeNodeValue nodeValues;

    private final HugeObjectArray<MpscLinkedQueue<Double>> messageQueues;

    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;

    public static <CONFIG extends PregelConfig> Pregel<CONFIG> create(
            Graph graph,
            CONFIG config,
            PregelComputation<CONFIG> computation,
            int batchSize,
            ExecutorService executor,
            AllocationTracker tracker
    ) {
        return new Pregel<>(
                graph,
                config,
                computation,
                CompositeNodeValue.of(computation.nodeSchema(), graph.nodeCount(), config.concurrency(), tracker),
                batchSize,
                executor,
                tracker
        );
    }

    // TODO: adapt for composite node value
    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(Pregel.class)
            .perNode("node values", HugeDoubleArray::memoryEstimation)
            .perNode("receiver bits", MemoryUsage::sizeOfBitset)
            .perNode("vote bits", MemoryUsage::sizeOfBitset)
            .perThread("compute steps", MemoryEstimations.builder(ComputeStep.class)
                .perNode("sender bits", MemoryUsage::sizeOfBitset)
                .build()
            )
            .add(
                "message queues",
                MemoryEstimations.setup("", (dimensions, concurrency) ->
                    MemoryEstimations.builder(HugeObjectArray.class)
                        .perNode("node queue", MemoryEstimations.builder(MpscLinkedQueue.class)
                            .fixed("messages", dimensions.averageDegree() * Double.BYTES)
                            .build()
                        )
                        .build()
                )
            )
            .build();
    }

    private Pregel(
            final Graph graph,
            final CONFIG config,
            final PregelComputation<CONFIG> computation,
            final CompositeNodeValue initialNodeValues,
            final int batchSize,
            final ExecutorService executor,
            final AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.computation = computation;
        this.nodeValues = initialNodeValues;
        this.batchSize = batchSize;
        this.concurrency = config.concurrency();
        this.executor = executor;

        this.messageQueues = initLinkedQueues(graph, tracker);
    }

    public PregelResult run() {
        int iterations = 0;
        boolean canHalt = false;
        // Tracks if a node received messages in the previous iteration
        // TODO: try RoaringBitSet instead
        BitSet receiverBits = new BitSet(graph.nodeCount());
        // Tracks if a node voted to halt in the previous iteration
        BitSet voteBits = new BitSet(graph.nodeCount());

        List<ComputeStep<CONFIG>> computeSteps = createComputeSteps();

        while (iterations < config.maxIterations() && !canHalt) {
            int iteration = iterations++;

            // Init compute steps with the updated state
            for (ComputeStep<CONFIG> computeStep : computeSteps) {
                computeStep.init(iteration, receiverBits, voteBits);
            }

            runComputeSteps(computeSteps, iteration, receiverBits);

            if (iteration > 0) {
                receiverBits.clear();
            }
            receiverBits = unionBitSets(computeSteps, receiverBits, ComputeStep::getSenders);

            // No messages have been sent
            if (receiverBits.nextSetBit(0) == -1) {
                canHalt = true;
            }
        }

        return ImmutablePregelResult.builder()
            .nodeValues(nodeValues)
            .didConverge(canHalt)
            .ranIterations(iterations)
            .build();
    }

    public void release() {
        messageQueues.release();
    }

    private List<ComputeStep<CONFIG>> createComputeSteps() {
        List<Partition> partitions = PartitionUtils.rangePartition(concurrency, graph.nodeCount());

        List<ComputeStep<CONFIG>> computeSteps = new ArrayList<>(concurrency);

        for (Partition partition : partitions) {
            computeSteps.add(new ComputeStep<>(
                graph,
                computation,
                config,
                0,
                partition,
                nodeValues,
                messageQueues,
                graph
            ));
        }
        return computeSteps;
    }

    private BitSet unionBitSets(Collection<ComputeStep<CONFIG>> computeSteps, BitSet identity, Function<ComputeStep<CONFIG>, BitSet> fn) {
        return ParallelUtil.parallelStream(computeSteps.stream(), concurrency, stream ->
                stream.map(fn).reduce(identity, (bitSet1, bitSet2) -> {
                    bitSet1.union(bitSet2);
                    return bitSet1;
                }));
    }

    private void runComputeSteps(
        Collection<ComputeStep<CONFIG>> computeSteps,
        final int iteration,
        BitSet messageBits
    ) {

        if (!config.isAsynchronous()) {
            // Synchronization barrier:
            // Add termination flag to message queues that
            // received messages in the previous iteration.
            if (iteration > 0) {
                ParallelUtil.parallelStreamConsume(
                        LongStream.range(0, graph.nodeCount()),
                        concurrency,
                        nodeIds -> nodeIds.forEach(nodeId -> {
                            if (messageBits.get(nodeId)) {
                                messageQueues.get(nodeId).add(TERMINATION_SYMBOL);
                            }
                        }));
            }
        }

        ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
    }

    @SuppressWarnings({"unchecked"})
    private HugeObjectArray<MpscLinkedQueue<Double>> initLinkedQueues(Graph graph, AllocationTracker tracker) {
        // sad java 😞
        Class<MpscLinkedQueue<Double>> queueClass = (Class<MpscLinkedQueue<Double>>) new MpscLinkedQueue<Double>().getClass();

        HugeObjectArray<MpscLinkedQueue<Double>> messageQueues = HugeObjectArray.newArray(
                queueClass,
                graph.nodeCount(),
                tracker);

        ParallelUtil.parallelStreamConsume(
                LongStream.range(0, graph.nodeCount()),
                concurrency,
                nodeIds -> nodeIds.forEach(nodeId -> messageQueues.set(nodeId, new MpscLinkedQueue<Double>())));

        return messageQueues;
    }

    public static final class ComputeStep<CONFIG extends PregelConfig> implements Runnable {

        private final long nodeCount;
        private final long relationshipCount;
        private final boolean isAsync;
        private final PregelComputation<CONFIG> computation;
        private final PregelContext.InitContext<CONFIG> initContext;
        private final PregelContext.ComputeContext<CONFIG> computeContext;
        private final BitSet senderBits;
        private final Partition nodeBatch;
        private final Degrees degrees;
        private final CompositeNodeValue nodeValues;
        private final HugeObjectArray<? extends Queue<Double>> messageQueues;
        private final RelationshipIterator relationshipIterator;

        private int iteration;
        private BitSet receiverBits;
        private BitSet voteBits;

        private ComputeStep(
            Graph graph,
            PregelComputation<CONFIG> computation,
            CONFIG config,
            int iteration,
            Partition nodeBatch,
            CompositeNodeValue nodeValues,
            HugeObjectArray<? extends Queue<Double>> messageQueues,
            RelationshipIterator relationshipIterator
        ) {
            this.iteration = iteration;
            this.nodeCount = graph.nodeCount();
            this.relationshipCount = graph.relationshipCount();
            this.isAsync = config.isAsynchronous();
            this.computation = computation;
            this.senderBits = new BitSet(nodeCount);
            this.nodeBatch = nodeBatch;
            this.degrees = graph;
            this.nodeValues = nodeValues;
            this.messageQueues = messageQueues;
            this.relationshipIterator = relationshipIterator.concurrentCopy();
            this.computeContext = PregelContext.computeContext(this, config);
            this.initContext = PregelContext.initContext(this, config, graph);
        }

        void init(int iteration, BitSet receiverBits, BitSet voteBits) {
            this.iteration = iteration;
            this.receiverBits = receiverBits;
            this.voteBits = voteBits;

            if (iteration > 0) {
                this.senderBits.clear();
            }
        }

        @Override
        public void run() {
            var messageIterator = isAsync
                ? new MessageIterator.Async()
                : new MessageIterator.Sync();
            var messages = new Messages(messageIterator);

            long batchStart = nodeBatch.startNode;
            long batchEnd = batchStart + nodeBatch.nodeCount;

            for (long nodeId = batchStart; nodeId < batchEnd; nodeId++) {

                if (computeContext.isInitialSuperstep()) {
                    computation.init(initContext, nodeId);
                }

                if (receiverBits.get(nodeId) || !voteBits.get(nodeId)) {
                    voteBits.clear(nodeId);

                    messageIterator.init(receiveMessages(nodeId));
                    computation.compute(computeContext, nodeId, messages);
                }
            }
        }

        BitSet getSenders() {
            return senderBits;
        }

        BitSet getVotes() {
            return voteBits;
        }

        public int getIteration() {
            return iteration;
        }

        long getNodeCount() {
            return nodeCount;
        }

        long getRelationshipCount() {
            return relationshipCount;
        }

        int getDegree(long nodeId) {
            return degrees.degree(nodeId);
        }

        void voteToHalt(long nodeId) {
            voteBits.set(nodeId);
        }

        void sendMessages(long nodeId, double message) {
            relationshipIterator.forEachRelationship(nodeId, (sourceNodeId, targetNodeId) -> {
                messageQueues.get(targetNodeId).add(message);
                senderBits.set(targetNodeId);
                return true;
            });
        }

        void sendWeightedMessages(long nodeId, double message) {
            relationshipIterator.forEachRelationship(nodeId, 1.0, (source, target, weight) -> {
                messageQueues.get(target).add(computation.applyRelationshipWeight(message, weight));
                senderBits.set(target);
                return true;
            });
        }

        private @Nullable Queue<Double> receiveMessages(long nodeId) {
            return receiverBits.get(nodeId) ? messageQueues.get(nodeId) : null;
        }

        double doubleNodeValue(String key, long nodeId) {
            return nodeValues.doubleProperties(key).get(nodeId);
        }

        long longNodeValue(String key, long nodeId) {
            return nodeValues.longProperties(key).get(nodeId);
        }

        void setNodeValue(String key, long nodeId, double value) {
            nodeValues.set(key, nodeId, value);
        }

        void setNodeValue(String key, long nodeId, long value) {
            nodeValues.set(key, nodeId, value);
        }
    }

    public static final class CompositeNodeValue {

        private final NodeSchema nodeSchema;
        private final Map<String, HugeDoubleArray> doubleProperties;
        private final Map<String, HugeLongArray> longProperties;

        static CompositeNodeValue of(NodeSchema nodeSchema, long nodeCount, int concurrency, AllocationTracker tracker) {
            Map<String, HugeDoubleArray> doubleProperties = new HashMap<>();
            Map<String, HugeLongArray> longProperties = new HashMap<>();

            nodeSchema.elements().forEach(element -> {
                if (element.propertyType() == ValueType.DOUBLE) {
                    var nodeValues = HugeDoubleArray.newArray(nodeCount, tracker);
                    ParallelUtil.parallelStreamConsume(
                        LongStream.range(0, nodeCount),
                        concurrency,
                        nodeIds -> nodeIds.forEach(nodeId -> nodeValues.set(nodeId, DefaultValue.DOUBLE_DEFAULT_FALLBACK))
                    );
                    doubleProperties.put(element.propertyKey(), nodeValues);
                } else if (element.propertyType() == ValueType.LONG) {
                    var nodeValues = HugeLongArray.newArray(nodeCount, tracker);
                    ParallelUtil.parallelStreamConsume(
                        LongStream.range(0, nodeCount),
                        concurrency,
                        nodeIds -> nodeIds.forEach(nodeId -> nodeValues.set(nodeId, DefaultValue.LONG_DEFAULT_FALLBACK))
                    );
                    longProperties.put(element.propertyKey(), nodeValues);
                } else {
                    throw new IllegalArgumentException(formatWithLocale("Unsupported value type: %s", element.propertyType()));
                }
            });

            return new CompositeNodeValue(nodeSchema, doubleProperties, longProperties);
        }

        private CompositeNodeValue(
            NodeSchema nodeSchema,
            Map<String, HugeDoubleArray> doubleProperties,
            Map<String, HugeLongArray> longProperties
        ) {
            this.nodeSchema = nodeSchema;
            this.doubleProperties = doubleProperties;
            this.longProperties = longProperties;
        }

        public NodeSchema schema() {
            return nodeSchema;
        }

        public HugeDoubleArray doubleProperties(String propertyKey) {
            return doubleProperties.get(propertyKey);
        }

        public HugeLongArray longProperties(String propertyKey) {
            return longProperties.get(propertyKey);
        }

        public double doubleValue(String key, long nodeId) {
            return doubleProperties.get(key).get(nodeId);
        }

        public long longValue(String key, long nodeId) {
            return longProperties.get(key).get(nodeId);
        }

        void set(String key, long nodeId, double value) {
            doubleProperties.get(key).set(nodeId, value);
        }

        void set(String key, long nodeId, long value) {
            longProperties.get(key).set(nodeId, value);
        }
    }

    public static class Messages implements Iterable<Double> {

        private final MessageIterator iterator;

        Messages(MessageIterator iterator) {
            this.iterator = iterator;
        }

        @NotNull
        @Override
        public Iterator<Double> iterator() {
            return iterator;
        }
    }

    abstract static class MessageIterator implements Iterator<Double> {

        Queue<Double> queue;

        @Nullable Double next;

        @Override
        public @Nullable Double next() {
            return next;
        }

        void init(@Nullable Queue<Double> queue) {
            this.queue = queue;
        }

        static class Sync extends MessageIterator {
            @Override
            public boolean hasNext() {
                if (queue == null) {
                    return false;
                }
                return !Double.isNaN(next = queue.poll());
            }
        }

        static class Async extends MessageIterator {
            @Override
            public boolean hasNext() {
                if (queue == null) {
                    return false;
                }
                return (next = queue.poll()) != null;
            }
        }
    }

    @ValueClass
    public interface NodeSchema {
        List<Element> elements();
    }

    @ValueClass
    public interface Element {
        String propertyKey();
        ValueType propertyType();
    }

    @Builder.Factory
    static NodeSchema nodeSchema(Map<String, ValueType> elements) {
        return ImmutableNodeSchema.of(elements.entrySet().stream()
            .map(entry -> ImmutableElement.of(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList())
        );
    }

    @ValueClass
    public interface PregelResult {

        CompositeNodeValue nodeValues();

        int ranIterations();

        boolean didConverge();
    }
}
