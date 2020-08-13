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
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.LazyMappingCollection;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongCollections;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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

    public static final String DEFAULT_NODE_VALUE_KEY = "nodeValue";
    public static final ValueType DEFAULT_NODE_VALUE_TYPE = ValueType.DOUBLE;

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

        // TODO: maybe try degree partitioning or clustering (better locality)
        Collection<PrimitiveLongIterable> nodeBatches = LazyBatchCollection.of(
                graph.nodeCount(),
                batchSize,
                (start, length) -> () -> PrimitiveLongCollections.range(start, start + length - 1L));

        while (iterations < config.maxIterations() && !canHalt) {
            int iteration = iterations++;

            final List<ComputeStep<CONFIG>> computeSteps = runComputeSteps(nodeBatches, iteration, receiverBits, voteBits);

            receiverBits = unionBitSets(computeSteps, ComputeStep::getSenders);
            voteBits = unionBitSets(computeSteps, ComputeStep::getVotes);

            // No messages have been sent
            if (receiverBits.nextSetBit(0) == -1) {
                canHalt = true;
            }
        }

        return ImmutablePregelResult.builder()
            .compositeNodeValues(nodeValues)
            .didConverge(canHalt)
            .ranIterations(iterations)
            .build();
    }

    public void release() {
        messageQueues.release();
    }

    private BitSet unionBitSets(Collection<ComputeStep<CONFIG>> computeSteps, Function<ComputeStep<CONFIG>, BitSet> fn) {
        return ParallelUtil.parallelStream(computeSteps.stream(), concurrency, stream ->
                stream.map(fn).reduce((bitSet1, bitSet2) -> {
                    bitSet1.union(bitSet2);
                    return bitSet1;
                }).orElseGet(BitSet::new));
    }

    private List<ComputeStep<CONFIG>> runComputeSteps(
            Collection<PrimitiveLongIterable> nodeBatches,
            final int iteration,
            BitSet messageBits,
            BitSet voteToHaltBits) {

        final List<ComputeStep<CONFIG>> tasks = new ArrayList<>(nodeBatches.size());

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

        Collection<ComputeStep<CONFIG>> computeSteps = LazyMappingCollection.of(
                nodeBatches,
                nodeBatch -> {
                    ComputeStep<CONFIG> task = new ComputeStep<>(
                        graph,
                        computation,
                        config,
                        iteration,
                        nodeBatch,
                        nodeValues,
                        messageBits,
                        voteToHaltBits,
                        messageQueues,
                        graph
                    );
                    tasks.add(task);
                    return task;
                });

        ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
        return tasks;
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

        private final int iteration;
        private final long nodeCount;
        private final long relationshipCount;
        private final PregelComputation<CONFIG> computation;
        private final PregelContext<CONFIG> pregelContext;
        private final BitSet senderBits;
        private final BitSet receiverBits;
        private final BitSet voteBits;
        private final PrimitiveLongIterable nodeBatch;
        private final Degrees degrees;
        private final CompositeNodeValue nodeValues;
        private final HugeObjectArray<? extends Queue<Double>> messageQueues;
        private final RelationshipIterator relationshipIterator;

        private ComputeStep(
            Graph graph,
            PregelComputation<CONFIG> computation,
            CONFIG config,
            int iteration,
            PrimitiveLongIterable nodeBatch,
            CompositeNodeValue nodeValues,
            BitSet receiverBits,
            BitSet voteBits,
            HugeObjectArray<? extends Queue<Double>> messageQueues,
            RelationshipIterator relationshipIterator
        ) {
            this.iteration = iteration;
            this.nodeCount = graph.nodeCount();
            this.relationshipCount = graph.relationshipCount();
            this.computation = computation;
            this.senderBits = new BitSet(nodeCount);
            this.receiverBits = receiverBits;
            this.voteBits = voteBits;
            this.nodeBatch = nodeBatch;
            this.degrees = graph;
            this.nodeValues = nodeValues;
            this.messageQueues = messageQueues;
            this.relationshipIterator = relationshipIterator.concurrentCopy();
            this.pregelContext = new PregelContext<>(this, config, graph);
        }

        @Override
        public void run() {
            final PrimitiveLongIterator nodesIterator = nodeBatch.iterator();

            while (nodesIterator.hasNext()) {
                final long nodeId = nodesIterator.next();

                if (pregelContext.isInitialSuperstep()) {
                    computation.init(pregelContext, nodeId);
                }

                if (receiverBits.get(nodeId) || !voteBits.get(nodeId)) {
                    voteBits.clear(nodeId);

                    computation.compute(pregelContext, nodeId, receiveMessages(nodeId));
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

        double getNodeValue(long nodeId) {
            return nodeValues.doubleValue(DEFAULT_NODE_VALUE_KEY, nodeId);
        }

        void setNodeValue(long nodeId, double value) {
            nodeValues.set(DEFAULT_NODE_VALUE_KEY, nodeId, value);
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

        private Queue<Double> receiveMessages(long nodeId) {
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

        double doubleValue(String key, long nodeId) {
            return doubleProperties.get(key).get(nodeId);
        }

        long longValue(String key, long nodeId) {
            return longProperties.get(key).get(nodeId);
        }

        void set(String key, long nodeId, double value) {
            doubleProperties.get(key).set(nodeId, value);
        }

        void set(String key, long nodeId, long value) {
            longProperties.get(key).set(nodeId, value);
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

        CompositeNodeValue compositeNodeValues();

        default HugeDoubleArray nodeValues() {
            return compositeNodeValues().doubleProperties(DEFAULT_NODE_VALUE_KEY);
        }

        int ranIterations();

        boolean didConverge();
    }
}
