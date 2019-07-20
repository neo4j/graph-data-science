package org.neo4j.graphalgo.pregel;

import com.carrotsearch.hppc.ArraySizingStrategy;
import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayList;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.LazyMappingCollection;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class Pregel {

    private final Graph graph;
    private final HugeWeightMapping nodeProperties;
    private final RelationshipWeights relationshipWeights;
    private final Computation computation;
    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final ProgressLogger progressLogger;

    public Pregel(
            final Graph graph,
            final HugeWeightMapping nodeProperties,
            final Computation computation,
            final int batchSize,
            final int concurrency,
            final ExecutorService executor,
            final AllocationTracker tracker,
            final ProgressLogger progressLogger) {
        this.graph = graph;
        this.nodeProperties = nodeProperties;
        this.relationshipWeights = graph;
        this.computation = computation;
        this.tracker = tracker;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = executor;
        this.progressLogger = progressLogger;
    }

    public int run(final int maxIterations) {
        int currentIteration = 0;
        BitSet hasMessage = new BitSet(graph.nodeCount());

        boolean canHalt = false;
        while (currentIteration < maxIterations && !canHalt) {
            final List<ComputeStep> computeSteps = runSuperstep(currentIteration++, hasMessage);

            if (computeSteps.parallelStream().map(ComputeStep::canHalt).reduce(true, (l, r) -> l && r)) {
                canHalt = true;
            } else {
                hasMessage.clear();
            }
        }
        return currentIteration;
    }

    private List<ComputeStep> runSuperstep(final int iteration, final BitSet hasMessage) {
        Collection<PrimitiveLongIterable> iterators = graph.batchIterables(batchSize);

        int threads = iterators.size();

        final List<ComputeStep> tasks = new ArrayList<>(threads);

        Collection<ComputeStep> computeSteps = LazyMappingCollection.of(
                iterators,
                nodeIterator -> {
                    ComputeStep task = new ComputeStep(
                            computation,
                            hasMessage,
                            iteration,
                            nodeIterator,
                            graph,
                            nodeProperties,
                            relationshipWeights,
                            graph,
                            progressLogger);
                    tasks.add(task);
                    return task;
                });

        ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
        return tasks;
    }

    public static final class ComputeStep implements Runnable {

        private final int iteration;
        private final Computation computation;
        private final BitSet hasMessage;
        private final PrimitiveLongIterable nodes;
        private final Degrees degrees;
        private final HugeWeightMapping nodeProperties;
        private final RelationshipWeights relationshipWeights;
        private final RelationshipIterator relationshipIterator;
        private final ProgressLogger progressLogger;

        private ComputeStep(
                final Computation computation,
                final BitSet hasMessage,
                final int iteration,
                final PrimitiveLongIterable nodes,
                final Degrees degrees,
                final HugeWeightMapping nodeProperties,
                final RelationshipWeights relationshipWeights,
                final RelationshipIterator relationshipIterator,
                final ProgressLogger progressLogger) {
            this.iteration = iteration;
            this.computation = computation;
            this.hasMessage = hasMessage;
            this.nodes = nodes;
            this.degrees = degrees;
            this.nodeProperties = nodeProperties;
            this.relationshipWeights = relationshipWeights;
            this.relationshipIterator = relationshipIterator.concurrentCopy();
            this.progressLogger = progressLogger;

            computation.setComputeStep(this);
        }

        @Override
        public void run() {
            final PrimitiveLongIterator nodesIterator = nodes.iterator();

            while (nodesIterator.hasNext()) {
                computation.compute(nodesIterator.next());
            }
        }

        public int getIteration() {
            return iteration;
        }

        int getDegree(final long nodeId) {
            return degrees.degree(nodeId, Direction.OUTGOING);
        }

        double getNodeValue(final long nodeId) {
            return nodeProperties.nodeWeight(nodeId);
        }

        void setNodeValue(final long nodeId, final double value) {
            nodeProperties.putNodeWeight(nodeId, value);
        }

        private static final ArraySizingStrategy ARRAY_SIZING_STRATEGY =
                (currentBufferLength, elementsCount, expectedAdditions) -> expectedAdditions + elementsCount;

        static double[] NO_MESSAGE = new double[0];

        double[] getMessages(final long nodeId) {
            if (!hasMessage.get(nodeId)) {
                return NO_MESSAGE;
            }
            final int degree = degrees.degree(nodeId, Direction.INCOMING);
            final DoubleArrayList doubleCursors = new DoubleArrayList(degree, ARRAY_SIZING_STRATEGY);

            relationshipIterator.forEachRelationship(
                    nodeId,
                    Direction.INCOMING,
                    (sourceNodeId, targetNodeId, weight) -> {
                        doubleCursors.add(weight);
                        return true;
                    });

            assert (doubleCursors.buffer.length == degree);
            assert (doubleCursors.elementsCount == degree);

            return doubleCursors.buffer;
        }

        void receiveMessages(final long nodeId, final double message) {
            relationshipIterator.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
                relationshipWeights.setWeight(sourceNodeId, targetNodeId, message);
                hasMessage.set(targetNodeId);
                return true;
            });
        }

        boolean canHalt() {
            boolean canHalt = true;
            final PrimitiveLongIterator nodes = this.nodes.iterator();
            while (nodes.hasNext()) {
                if (hasMessage.get(nodes.next())) {
                    canHalt = false;
                    break;
                }
            }
            return canHalt;
        }
    }
}
