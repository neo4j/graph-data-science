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
package org.neo4j.gds.similarity.nodesim;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.relationships.RelationshipConsumer;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.SetBitsIterable;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.BatchingProgressLogger;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.wcc.Wcc;
import org.neo4j.gds.wcc.WccAlgorithmFactory;
import org.neo4j.gds.wcc.WccParameters;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class NodeSimilarity extends Algorithm<NodeSimilarityResult> {

    private final Graph graph;
    private final NodeSimilarityParameters parameters;
    private final boolean sortVectors;
    private final boolean weighted;

    private final BitSet sourceNodes;
    private final BitSet targetNodes;
    private final NodeFilter sourceNodeFilter;
    private final NodeFilter targetNodeFilter;

    private final ExecutorService executorService;
    private final Concurrency concurrency;
    private final MetricSimilarityComputer similarityComputer;

    private HugeObjectArray<long[]> neighbors;
    private HugeObjectArray<double[]> weights;
    private LongUnaryOperator components;
    private Function<Long, LongStream> sourceNodesStream;
    private BiFunction<Long, Long, LongStream> targetNodesStream;

    /**
     * @deprecated Don't use this, use the one that injects termination flag directly
     */
    @Deprecated
    public NodeSimilarity(
        Graph graph,
        NodeSimilarityParameters parameters,
        Concurrency concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        this(
            graph,
            parameters,
            concurrency,
            executorService,
            progressTracker,
            NodeFilter.ALLOW_EVERYTHING,
            NodeFilter.ALLOW_EVERYTHING,
            TerminationFlag.RUNNING_TRUE
        );
    }

    public NodeSimilarity(
        Graph graph,
        NodeSimilarityParameters parameters,
        Concurrency concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this(
            graph,
            parameters,
            concurrency,
            executorService,
            progressTracker,
            NodeFilter.ALLOW_EVERYTHING,
            NodeFilter.ALLOW_EVERYTHING,
            terminationFlag
        );
    }

    /**
     * @deprecated Don't use this, use the one that injects termination flag directly
     */
    @Deprecated
    public NodeSimilarity(
        Graph graph,
        NodeSimilarityParameters parameters,
        Concurrency concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        NodeFilter sourceNodeFilter,
        NodeFilter targetNodeFilter
    ) {
        this(
            graph,
            parameters,
            concurrency,
            executorService,
            progressTracker,
            sourceNodeFilter,
            targetNodeFilter,
            TerminationFlag.RUNNING_TRUE
        );
    }

    public NodeSimilarity(
        Graph graph,
        NodeSimilarityParameters parameters,
        Concurrency concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        NodeFilter sourceNodeFilter,
        NodeFilter targetNodeFilter,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.graph = graph;
        this.sortVectors = graph.schema().relationshipSchema().availableTypes().size() > 1;
        this.sourceNodeFilter = sourceNodeFilter;
        this.targetNodeFilter = targetNodeFilter;
        this.concurrency = concurrency;
        this.parameters = parameters;
        this.similarityComputer = parameters.similarityComputer();
        this.executorService = executorService;
        this.sourceNodes = new BitSet(graph.nodeCount());
        this.targetNodes = new BitSet(graph.nodeCount());
        this.weighted = this.parameters.hasRelationshipWeightProperty();
        this.terminationFlag = terminationFlag;
    }

    @Override
    public NodeSimilarityResult compute() {
        progressTracker.beginSubTask();

        prepare();

        if (parameters.computeToStream()) {
            var computeToStream = computeToStream();
            progressTracker.endSubTask();
            return ImmutableNodeSimilarityResult.of(
                Optional.of(computeToStream),
                Optional.empty()
            );
        } else {
            var computeToGraph = computeToGraph();
            progressTracker.endSubTask();
            return ImmutableNodeSimilarityResult.of(
                Optional.empty(),
                Optional.of(computeToGraph)
            );
        }
    }

    private Stream<SimilarityResult> computeToStream() {
        // Create a filter for which nodes to compare and calculate the neighborhood for each node
        terminationFlag.assertRunning();

        // Compute similarities
        if (parameters.hasTopN() && !parameters.hasTopK()) {
            // Special case: compute topN without topK.
            // This can not happen when algo is called from proc.
            // Ignore parallelism, always run single threaded,
            // but run on primitives.
            return computeTopN();
        } else {
            return concurrency.value() > 1
                ? computeParallel()
                : computeSimilarityResultStream();
        }
    }

    private SimilarityGraphResult computeToGraph() {
        Graph similarityGraph;
        boolean isTopKGraph = false;

        if (parameters.hasTopK() && !parameters.hasTopN()) {
            terminationFlag.assertRunning();

            TopKMap topKMap = concurrency.value() > 1
                ? computeTopKMapParallel()
                : computeTopKMap();

            isTopKGraph = true;
            similarityGraph = new TopKGraph(graph, topKMap);
        } else {
            Stream<SimilarityResult> similarities = computeToStream();
            similarityGraph = new SimilarityGraphBuilder(
                graph,
                concurrency,
                executorService,
                terminationFlag
            ).build(similarities);
        }
        return new SimilarityGraphResult(similarityGraph, sourceNodes.cardinality(), isTopKGraph);
    }

    private void prepare() {
        progressTracker.beginSubTask();

        components = initComponents();
        if (parameters.runWCC()) {
            progressTracker.beginSubTask();
        }
        initNodeSpecificFields();

        sourceNodesStream = initSourceNodesStream();

        targetNodesStream = initTargetNodesStream();

        if (parameters.runWCC()) {
            progressTracker.endSubTask();
        }
        progressTracker.endSubTask();
    }

    private Stream<SimilarityResult> computeSimilarityResultStream() {
        if (parameters.hasTopK()) {
            var topKMap = computeTopKMap();
            return parameters.hasTopN() ? computeTopN(topKMap) : topKMap.stream();
        } else {
            return computeAll();
        }
    }

    private Stream<SimilarityResult> computeParallel() {
        if (parameters.hasTopK()) {
            var topKMap = computeTopKMapParallel();
            return parameters.hasTopN() ? computeTopN(topKMap) : topKMap.stream();
        } else {
            return computeAllParallel();
        }
    }

    private LongUnaryOperator initComponents() {
        if (!parameters.useComponents()) {
            // considering everything as within the same component
            return n -> 0;
        }

        if (parameters.componentProperty() != null) {
            // extract component info from property
            NodePropertyValues nodeProperties = graph.nodeProperties(parameters.componentProperty());
            return initComponentIdMapping(graph, nodeProperties::longValue);
        }

        // run WCC to determine components
        progressTracker.beginSubTask();
        var wccParameters = new WccParameters(0D, concurrency);
        Wcc wcc = new WccAlgorithmFactory<>().build(graph, wccParameters, ProgressTracker.NULL_TRACKER);
        DisjointSetStruct disjointSets = wcc.compute();
        progressTracker.endSubTask();
        return disjointSets::setIdOf;
    }

    private void initNodeSpecificFields() {
        neighbors = HugeObjectArray.newArray(long[].class, graph.nodeCount());
        if (weighted) {
            weights = HugeObjectArray.newArray(double[].class, graph.nodeCount());
        }

        DegreeComputer degreeComputer = new DegreeComputer();
        VectorComputer vectorComputer = VectorComputer.of(graph, weighted);
        DegreeFilter degreeFilter = new DegreeFilter(parameters.degreeCutoff(), parameters.upperDegreeCutoff());
        neighbors.setAll(node -> {
            graph.forEachRelationship(node, degreeComputer);
            int degree = degreeComputer.degree;
            degreeComputer.reset();
            vectorComputer.reset(degree);

            progressTracker.logProgress(graph.degree(node));
            if (degreeFilter.apply(degree)) {
                if (sourceNodeFilter.test(node)) {
                    sourceNodes.set(node);
                }
                if (targetNodeFilter.test(node)) {
                    targetNodes.set(node);
                }

                // TODO: we don't need to do the rest of the prepare for a node that isn't going to be used in the computation
                vectorComputer.forEachRelationship(node);

                if (sortVectors) {
                    vectorComputer.sortTargetIds();
                }
                if (weighted) {
                    weights.set(node, vectorComputer.getWeights());
                }
                return vectorComputer.targetIds.buffer;
            }
            return null;
        });
    }

    private Stream<SimilarityResult> computeAll() {
        progressTracker.beginSubTask(calculateWorkload());

        var similarityResultStream = loggableAndTerminableSourceNodeStream()
            .boxed()
            .flatMap(this::computeSimilaritiesForNode);
        progressTracker.endSubTask();
        return similarityResultStream;
    }

    private Stream<SimilarityResult> computeAllParallel() {
        return ParallelUtil.parallelStream(
            loggableAndTerminableSourceNodeStream(), concurrency, stream -> stream
                .boxed()
                .flatMap(this::computeSimilaritiesForNode)
        );
    }

    private TopKMap computeTopKMap() {
        progressTracker.beginSubTask(calculateWorkload());

        var comparator = parameters.normalizedK() > 0
            ? SimilarityResult.DESCENDING
            : SimilarityResult.ASCENDING;
        var topKMap = new TopKMap(neighbors.size(), sourceNodes, Math.abs(parameters.normalizedK()), comparator);

        loggableAndTerminableSourceNodeStream()
            .forEach(sourceNodeId -> {
                if (sourceNodeFilter.equals(NodeFilter.ALLOW_EVERYTHING)) {
                    targetNodesStream.apply(components.applyAsLong(sourceNodeId), sourceNodeId + 1)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId,
                            (source, target, similarity) -> {
                                topKMap.put(source, target, similarity);
                                topKMap.put(target, source, similarity);
                            }
                        ));
                } else {
                    targetNodesStream.apply(components.applyAsLong(sourceNodeId), 0L)
                        .filter(targetNodeId -> sourceNodeId != targetNodeId)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId, topKMap::put));
                }
            });
        progressTracker.endSubTask();
        return topKMap;
    }

    private TopKMap computeTopKMapParallel() {
        progressTracker.beginSubTask(calculateWorkload());

        var comparator = parameters.normalizedK() > 0
            ? SimilarityResult.DESCENDING
            : SimilarityResult.ASCENDING;
        var topKMap = new TopKMap(neighbors.size(), sourceNodes, Math.abs(parameters.normalizedK()), comparator);

        ParallelUtil.parallelStreamConsume(
            loggableAndTerminableSourceNodeStream(),
            concurrency,
            terminationFlag,
            stream -> stream
                .forEach(sourceNodeId ->
                    // We deliberately compute the full matrix (except the diagonal).
                    // The parallel workload is partitioned based on the outer stream.
                    // The TopKMap stores a priority queue for each node. Writing
                    // into these queues is not considered to be thread-safe.
                    // Hence, we need to ensure that down the stream, exactly one queue
                    // within the TopKMap processes all pairs for a single node.
                    targetNodesStream.apply(components.applyAsLong(sourceNodeId), 0L)
                        .filter(targetNodeId -> sourceNodeId != targetNodeId)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId, topKMap::put))
                )
        );

        progressTracker.endSubTask();
        return topKMap;
    }

    private Stream<SimilarityResult> computeTopN() {
        progressTracker.beginSubTask(calculateWorkload());

        var topNList = new TopNList(parameters.normalizedN());
        loggableAndTerminableSourceNodeStream()
            .forEach(sourceNodeId -> {
                if (sourceNodeFilter.equals(NodeFilter.ALLOW_EVERYTHING)) {
                    targetNodesStream.apply(components.applyAsLong(sourceNodeId), sourceNodeId + 1)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId, topNList::add));
                } else {
                    targetNodesStream.apply(components.applyAsLong(sourceNodeId), 0L)
                        .filter(targetNodeId -> sourceNodeId != targetNodeId)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId, topNList::add));
                }
            });

        progressTracker.endSubTask();
        return topNList.stream();
    }

    private Stream<SimilarityResult> computeTopN(TopKMap topKMap) {
        var topNList = new TopNList(parameters.normalizedN());
        topKMap.forEach(topNList::add);
        return topNList.stream();
    }

    private Function<Long, LongStream> initSourceNodesStream() {
        return offset -> new SetBitsIterable(sourceNodes, offset).stream();
    }

    private BiFunction<Long, Long, LongStream> initTargetNodesStream() {
        if (!parameters.useComponents()) {
            return (componentId, offset) -> new SetBitsIterable(targetNodes, offset).stream();
        }

        var componentNodes = ComponentNodes.create(components, targetNodes::get, graph.nodeCount(), concurrency);
        return (componentId, offset) -> StreamSupport
            .longStream(componentNodes.spliterator(componentId, offset), false);
    }

    private LongStream loggableAndTerminableSourceNodeStream() {
        return checkProgress(sourceNodesStream.apply(0L));
    }

    private Stream<SimilarityResult> computeSimilaritiesForNode(long sourceNodeId) {
        return targetNodesStream.apply(components.applyAsLong(sourceNodeId), sourceNodeId + 1)
            .mapToObj(targetNodeId -> {
                var resultHolder = new SimilarityResult[]{null};
                computeSimilarityFor(
                    sourceNodeId,
                    targetNodeId,
                    (source, target, similarity) -> resultHolder[0] = new SimilarityResult(source, target, similarity)
                );
                return resultHolder[0];
            })
            .filter(Objects::nonNull);
    }

    private static LongUnaryOperator initComponentIdMapping(Graph graph, LongUnaryOperator originComponentIdMapper) {
        var componentIdMappings = new HugeLongLongMap();
        var mappedComponentId = new AtomicLong(0L);
        var mappedComponentIdPerNode = HugeLongArray.newArray(graph.nodeCount());
        graph.forEachNode(n -> {
            long originComponentIdForNode = originComponentIdMapper.applyAsLong(n);
            long mappedComponentIdForNode = componentIdMappings.getOrDefault(originComponentIdMapper.applyAsLong(n),
                mappedComponentId.getAndIncrement());

            if (!componentIdMappings.containsKey(originComponentIdForNode)) {
                componentIdMappings.put(originComponentIdForNode, mappedComponentIdForNode);
            }
            mappedComponentIdPerNode.set(n, mappedComponentIdForNode);
            return true;
        });

        return mappedComponentIdPerNode::get;
    }

    interface SimilarityConsumer {
        void accept(long sourceNodeId, long targetNodeId, double similarity);
    }

    private void computeSimilarityFor(long sourceNodeId, long targetNodeId, SimilarityConsumer consumer) {
        double similarity;
        var sourceNodeNeighbors = neighbors.get(sourceNodeId);
        var targetNodeNeighbors = neighbors.get(targetNodeId);
        if (weighted) {
            similarity = computeWeightedSimilarity(
                sourceNodeNeighbors, targetNodeNeighbors, weights.get(sourceNodeId), weights.get(targetNodeId)
            );
        } else {
            similarity = computeSimilarity(sourceNodeNeighbors, targetNodeNeighbors);
        }
        if (!Double.isNaN(similarity)) {
            consumer.accept(sourceNodeId, targetNodeId, similarity);
        }
    }

    private double computeWeightedSimilarity(
        long[] sourceNodeNeighbors,
        long[] targetNodeNeighbors,
        double[] sourceNodeWeights,
        double[] targetNodeWeights
    ) {
        double similarity = similarityComputer.computeWeightedSimilarity(
            sourceNodeNeighbors,
            targetNodeNeighbors,
            sourceNodeWeights,
            targetNodeWeights
        );
        progressTracker.logProgress();
        return similarity;
    }

    private double computeSimilarity(long[] sourceNodeNeighbors, long[] targetNodeNeighbors) {
        double similarity = similarityComputer.computeSimilarity(sourceNodeNeighbors, targetNodeNeighbors);
        progressTracker.logProgress();
        return similarity;
    }

    private LongStream checkProgress(LongStream stream) {
        return stream.peek(node -> {
            if ((node & BatchingProgressLogger.MAXIMUM_LOG_INTERVAL) == 0) {
                terminationFlag.assertRunning();
            }
        });
    }

    private long calculateWorkload() {
        //for each source node, examine all their target nodes
        //if no filter then sourceNodes == targetNodes
        long workload = sourceNodes.cardinality() * targetNodes.cardinality();

        //when on concurrency of 1 on not-filtered similarity,  we only compare nodeId with greater indexed nodes
        // so work is halved. This does not hold for filtered similarity, since the targetNodes might be lesser indexed.
        boolean isNotFiltered = sourceNodeFilter.equals(NodeFilter.ALLOW_EVERYTHING) && targetNodeFilter.equals(
            NodeFilter.ALLOW_EVERYTHING);
        if (concurrency.value() == 1 && isNotFiltered) {
            workload = workload / 2;
        }
        return workload;
    }

    private static final class DegreeComputer implements RelationshipConsumer {

        long lastTarget = -1;
        int degree = 0;

        @Override
        public boolean accept(long source, long target) {
            if (source != target && lastTarget != target) {
                degree++;
            }
            lastTarget = target;
            return true;
        }

        void reset() {
            lastTarget = -1;
            degree = 0;
        }
    }
}
