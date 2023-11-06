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
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.SetBitsIterable;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.BatchingProgressLogger;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.wcc.ImmutableWccStreamConfig;
import org.neo4j.gds.wcc.Wcc;
import org.neo4j.gds.wcc.WccAlgorithmFactory;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class NodeSimilarity extends Algorithm<NodeSimilarityResult> {

    private final Graph graph;
    private final boolean sortVectors;
    private final NodeSimilarityBaseConfig config;

    private final BitSet sourceNodes;
    private final BitSet targetNodes;
    private final NodeFilter sourceNodeFilter;
    private final NodeFilter targetNodeFilter;

    private final ExecutorService executorService;
    private final int concurrency;
    private final MetricSimilarityComputer similarityComputer;
    private HugeObjectArray<long[]> neighbors;
    private HugeObjectArray<double[]> weights;
    private DisjointSetStruct components;

    private final boolean weighted;

    public static NodeSimilarity create(
        Graph graph,
        NodeSimilarityBaseConfig config,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        var similarityComputer = config.similarityMetric().build(config.similarityCutoff());
        return new NodeSimilarity(
            graph,
            config,
            similarityComputer,
            config.concurrency(),
            executorService,
            progressTracker
        );
    }

    public NodeSimilarity(
        Graph graph,
        NodeSimilarityBaseConfig config,
        MetricSimilarityComputer similarityComputer,
        int concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        this(
            graph,
            config,
            similarityComputer,
            NodeFilter.noOp,
            NodeFilter.noOp,
            concurrency,
            executorService,
            progressTracker
        );
    }

    public NodeSimilarity(
        Graph graph,
        NodeSimilarityBaseConfig config,
        MetricSimilarityComputer similarityComputer,
        NodeFilter sourceNodeFilter,
        NodeFilter targetNodeFilter,
        int concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.sortVectors = graph.schema().relationshipSchema().availableTypes().size() > 1;
        this.sourceNodeFilter = sourceNodeFilter;
        this.targetNodeFilter = targetNodeFilter;
        this.concurrency = concurrency;
        this.config = config;
        this.similarityComputer = similarityComputer;
        this.executorService = executorService;
        this.sourceNodes = new BitSet(graph.nodeCount());
        this.targetNodes = new BitSet(graph.nodeCount());
        this.weighted = config.hasRelationshipWeightProperty();
    }

    @Override
    public NodeSimilarityResult compute() {
        progressTracker.beginSubTask();

        prepare();

        if (config.computeToStream()) {
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
        if (config.hasTopN() && !config.hasTopK()) {
            // Special case: compute topN without topK.
            // This can not happen when algo is called from proc.
            // Ignore parallelism, always run single threaded,
            // but run on primitives.
            return computeTopN();
        } else {
            return config.isParallel()
                ? computeParallel()
                : computeSimilarityResultStream();
        }
    }

    private SimilarityGraphResult computeToGraph() {
        Graph similarityGraph;
        boolean isTopKGraph = false;

        if (config.hasTopK() && !config.hasTopN()) {
            terminationFlag.assertRunning();

            TopKMap topKMap = config.isParallel()
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

        computeComponents();

        neighbors = HugeObjectArray.newArray(long[].class, graph.nodeCount());
        if (weighted) {
            weights = HugeObjectArray.newArray(double[].class, graph.nodeCount());
        }

        DegreeComputer degreeComputer = new DegreeComputer();
        VectorComputer vectorComputer = VectorComputer.of(graph, weighted);
        DegreeFilter degreeFilter = new DegreeFilter(config.degreeCutoff(), config.upperDegreeCutoff());
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
        progressTracker.endSubTask();
    }

    private Stream<SimilarityResult> computeSimilarityResultStream() {
        if (config.hasTopK()) {
            var topKMap = computeTopKMap();
            return config.hasTopN() ? computeTopN(topKMap) : topKMap.stream();
        } else {
            return computeAll();
        }
    }

    private Stream<SimilarityResult> computeParallel() {
        if (config.hasTopK()) {
            var topKMap = computeTopKMapParallel();
            return config.hasTopN() ? computeTopN(topKMap) : topKMap.stream();
        } else {
            return computeAllParallel();
        }
    }

    private void computeComponents() {
        WccStreamConfig wccConfig = ImmutableWccStreamConfig
            .builder()
            .concurrency(concurrency)
            .addAllRelationshipTypes(config.relationshipTypes())
            .addAllNodeLabels(config.nodeLabels())
            .build();

        Wcc wcc = new WccAlgorithmFactory<>().build(graph, wccConfig, ProgressTracker.NULL_TRACKER);
        components = wcc.compute();
    }

    private Stream<SimilarityResult> computeAll() {
        progressTracker.beginSubTask(calculateWorkload());

        var similarityResultStream = loggableAndTerminatableSourceNodeStream()
            .boxed()
            .flatMap(this::computeSimilaritiesForNode);
        progressTracker.endSubTask();
        return similarityResultStream;
    }

    private Stream<SimilarityResult> computeAllParallel() {
        return ParallelUtil.parallelStream(
            loggableAndTerminatableSourceNodeStream(), concurrency, stream -> stream
                .boxed()
                .flatMap(this::computeSimilaritiesForNode)
        );
    }

    private TopKMap computeTopKMap() {
        progressTracker.beginSubTask(calculateWorkload());

        var comparator = config.normalizedK() > 0
            ? SimilarityResult.DESCENDING
            : SimilarityResult.ASCENDING;
        var topKMap = new TopKMap(neighbors.size(), sourceNodes, Math.abs(config.normalizedK()), comparator);

        loggableAndTerminatableSourceNodeStream()
            .forEach(sourceNodeId -> {
                if (sourceNodeFilter.equals(NodeFilter.noOp)) {
                    targetNodesStream(sourceNodeId + 1)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId,
                            (source, target, similarity) -> {
                                topKMap.put(source, target, similarity);
                                topKMap.put(target, source, similarity);
                            }
                        ));
                } else {
                    targetNodesStream()
                        .filter(targetNodeId -> sourceNodeId != targetNodeId)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId, topKMap::put));
                }
            });
        progressTracker.endSubTask();
        return topKMap;
    }

    private TopKMap computeTopKMapParallel() {
        progressTracker.beginSubTask(calculateWorkload());

        var comparator = config.normalizedK() > 0
            ? SimilarityResult.DESCENDING
            : SimilarityResult.ASCENDING;
        var topKMap = new TopKMap(neighbors.size(), sourceNodes, Math.abs(config.normalizedK()), comparator);

        ParallelUtil.parallelStreamConsume(
            loggableAndTerminatableSourceNodeStream(),
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
                    targetNodesStream()
                        .filter(targetNodeId -> sourceNodeId != targetNodeId)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId, topKMap::put))
                )
        );

        progressTracker.endSubTask();
        return topKMap;
    }

    private Stream<SimilarityResult> computeTopN() {
        progressTracker.beginSubTask(calculateWorkload());

        var topNList = new TopNList(config.normalizedN());
        loggableAndTerminatableSourceNodeStream()
            .forEach(sourceNodeId -> {
                if (sourceNodeFilter.equals(NodeFilter.noOp)) {
                    targetNodesStream(sourceNodeId + 1)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId, topNList::add));
                } else {
                    targetNodesStream()
                        .filter(targetNodeId -> sourceNodeId != targetNodeId)
                        .forEach(targetNodeId -> computeSimilarityFor(sourceNodeId, targetNodeId, topNList::add));
                }
            });

        progressTracker.endSubTask();
        return topNList.stream();
    }

    private Stream<SimilarityResult> computeTopN(TopKMap topKMap) {
        var topNList = new TopNList(config.normalizedN());
        topKMap.forEach(topNList::add);
        return topNList.stream();
    }

    private LongStream sourceNodesStream(long offset) {
        return new SetBitsIterable(sourceNodes, offset).stream();
    }

    private LongStream sourceNodesStream() {
        return sourceNodesStream(0);
    }

    private LongStream loggableAndTerminatableSourceNodeStream() {
        return checkProgress(sourceNodesStream());
    }

    private LongStream targetNodesStream(long offset) {
        return new SetBitsIterable(targetNodes, offset).stream();
    }

    private LongStream targetNodesStream() {
        return targetNodesStream(0);
    }

    private Stream<SimilarityResult> computeSimilaritiesForNode(long sourceNodeId) {
        return targetNodesStream(sourceNodeId + 1)
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

    interface SimilarityConsumer {
        void accept(long sourceNodeId, long targetNodeId, double similarity);
    }

    private void computeSimilarityFor(long sourceNodeId, long targetNodeId, SimilarityConsumer consumer) {
        double similarity = 0;
        if (components.setIdOf(sourceNodeId) != components.setIdOf(targetNodeId)) {
            consumer.accept(sourceNodeId, targetNodeId, similarity);
            return;
        }

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
        boolean isNotFiltered = sourceNodeFilter.equals(NodeFilter.noOp) && targetNodeFilter.equals(NodeFilter.noOp);
        if (concurrency == 1 && isNotFiltered) {
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
