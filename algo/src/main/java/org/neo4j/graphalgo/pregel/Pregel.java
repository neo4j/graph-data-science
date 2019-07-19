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
    private final BitSet votes;
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
        this.votes = new BitSet(graph.nodeCount());
        this.tracker = tracker;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = executor;
        this.progressLogger = progressLogger;
    }

    public int run(final int maxIterations) {
        int currentIteration = 0;

        while (currentIteration < maxIterations) {
            final List<ComputeStep> computeSteps = runSuperstep(currentIteration);
            currentIteration++;
            if (computeSteps.parallelStream().map(ComputeStep::canHalt).reduce(true, (l, r) -> l && r)) {
                break;
            } else {
                votes.clear();
            }
        }

        return currentIteration;
    }

    private List<ComputeStep> runSuperstep(final int iteration) {
        Collection<PrimitiveLongIterable> iterators = graph.batchIterables(batchSize);

        int threads = iterators.size();

        final List<ComputeStep> tasks = new ArrayList<>(threads);

        Collection<ComputeStep> computeSteps = LazyMappingCollection.of(
                iterators,
                nodeIterator -> {
                    ComputeStep task = new ComputeStep(
                            computation,
                            votes,
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

    public static class ComputeStep implements Runnable {

        private final int iteration;
        private final Computation computation;
        private final BitSet votes;
        private final PrimitiveLongIterable nodes;
        private final Degrees degrees;
        private final HugeWeightMapping nodeProperties;
        private final RelationshipWeights relationshipWeights;
        private final RelationshipIterator relationshipIterator;
        private final ProgressLogger progressLogger;

        private ComputeStep(
                final Computation computation,
                final BitSet votes,
                final int iteration,
                final PrimitiveLongIterable nodes,
                final Degrees degrees,
                final HugeWeightMapping nodeProperties,
                final RelationshipWeights relationshipWeights,
                final RelationshipIterator relationshipIterator,
                final ProgressLogger progressLogger) {
            this.iteration = iteration;
            this.computation = computation;
            this.votes = votes;
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
//            System.out.println("Running iteration: " + iteration);

            final PrimitiveLongIterator nodesIterator = nodes.iterator();

            while (nodesIterator.hasNext()) {
                computation.compute(nodesIterator.next());
            }
        }

        public int getIteration() {
            return iteration;
        }

        double getNodeValue(final long nodeId) {
            return nodeProperties.nodeWeight(nodeId);
        }

        void setNodeValue(final long nodeId, final double value) {
            nodeProperties.putNodeWeight(nodeId, value);
        }

        private static final ArraySizingStrategy ARRAY_SIZING_STRATEGY =
                (currentBufferLength, elementsCount, expectedAdditions) -> expectedAdditions + elementsCount;

        double[] getMessages(final long nodeId) {
            final int degree = degrees.degree(nodeId, Direction.BOTH);
            final DoubleArrayList doubleCursors = new DoubleArrayList(degree, ARRAY_SIZING_STRATEGY);

            relationshipIterator.forEachRelationship(nodeId, Direction.BOTH, (sourceNodeId, targetNodeId, weight) -> {
//                System.out.println(String.format("[%d (%d)] Weight: (%d)-[%.1f]->(%d)", nodeId, degree, sourceNodeId, weight, targetNodeId));
                doubleCursors.add(weight);
                return true;
            });

            assert (doubleCursors.buffer.length == degree);
            assert (doubleCursors.elementsCount == degree);

            return doubleCursors.buffer;
        }

        void sendToNeighbors(final long nodeId, final double message) {
            relationshipIterator.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
//                System.out.println(String.format("[%d] Message: (%d)-[%.1f]->(%d)", nodeId, sourceNodeId, message, targetNodeId));
                relationshipWeights.setWeight(sourceNodeId, targetNodeId, message);
                votes.set(targetNodeId);
                return true;
            });

            relationshipIterator.forEachRelationship(nodeId, Direction.INCOMING, (sourceNodeId, targetNodeId) -> {
//                System.out.println(String.format("[%d] Message: (%d)<-[%.1f]-(%d)", nodeId, sourceNodeId, message, targetNodeId));
                relationshipWeights.setWeight(targetNodeId, sourceNodeId, message);
                votes.set(sourceNodeId);
                return true;
            });
        }

        boolean canHalt() {
            boolean canHalt = true;
            final PrimitiveLongIterator nodes = this.nodes.iterator();
            while (nodes.hasNext()) {
                if (!votes.get(nodes.next())) {
                    canHalt = false;
                    break;
                }
            }
            return canHalt;
        }
    }
}
