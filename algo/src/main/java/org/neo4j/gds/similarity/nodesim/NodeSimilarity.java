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
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.SetBitsIterable;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filtering.NodeFilter;

import java.util.Arrays;
import java.util.Comparator;
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
    private HugeObjectArray<long[]> vectors;
    private HugeObjectArray<double[]> weights;
    private long nodesToCompare;

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
    public void release() {
        graph.release();
    }

    @Override
    public NodeSimilarityResult compute() {
        progressTracker.beginSubTask();
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

    public Stream<SimilarityResult> computeToStream() {
        // Create a filter for which nodes to compare and calculate the neighborhood for each node
        prepare();
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

    public SimilarityGraphResult computeToGraph() {
        Graph similarityGraph;
        boolean isTopKGraph = false;

        if (config.hasTopK() && !config.hasTopN()) {
            prepare();
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
                executorService
            ).build(similarities);
        }
        return new SimilarityGraphResult(similarityGraph, nodesToCompare, isTopKGraph);
    }

    private void prepare() {
        progressTracker.beginSubTask();

        vectors = HugeObjectArray.newArray(long[].class, graph.nodeCount());
        if (weighted) {
            weights = HugeObjectArray.newArray(double[].class, graph.nodeCount());
        }

        DegreeComputer degreeComputer = new DegreeComputer();
        VectorComputer vectorComputer = VectorComputer.of(graph, weighted);
        vectors.setAll(node -> {
            graph.forEachRelationship(node, degreeComputer);
            int degree = degreeComputer.degree;
            degreeComputer.reset();
            vectorComputer.reset(degree);

            if (degree >= config.degreeCutoff()) {
                if (sourceNodeFilter.test(node)) {
                    sourceNodes.set(node);
                }
                if (targetNodeFilter.test(node)) {
                    targetNodes.set(node);
                }

                // TODO: we don't need to do the rest of the prepare for a node that isn't going to be used in the computation
                progressTracker.logProgress(graph.degree(node));
                vectorComputer.forEachRelationship(node);
                if (weighted) {
                    weights.set(node, vectorComputer.getWeights());
                }
                if (sortVectors) {
                    Arrays.sort(vectorComputer.targetIds.buffer);
                }
                return vectorComputer.targetIds.buffer;
            }

            progressTracker.logProgress(graph.degree(node));
            return null;
        });
        nodesToCompare = sourceNodes.cardinality();
        progressTracker.endSubTask();
    }

    private Stream<SimilarityResult> computeSimilarityResultStream() {
        return (config.hasTopK() && config.hasTopN())
            ? computeTopN(computeTopKMap())
            : (config.hasTopK())
                ? computeTopKMap().stream()
                : computeAll();
    }

    private Stream<SimilarityResult> computeParallel() {
        return (config.hasTopK() && config.hasTopN())
            ? computeTopN(computeTopKMapParallel())
            : (config.hasTopK())
                ? computeTopKMapParallel().stream()
                : computeAllParallel();
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

        Comparator<SimilarityResult> comparator = config.normalizedK() > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        TopKMap topKMap = new TopKMap(vectors.size(), sourceNodes, Math.abs(config.normalizedK()), comparator);
        loggableAndTerminatableSourceNodeStream()
            .forEach(node1 -> {
                long[] vector1 = vectors.get(node1);
                if (sourceNodeFilter.equals(NodeFilter.noOp)) {
                    targetNodesStream(node1 + 1)
                        .forEach(node2 -> {
                            double similarity = weighted
                                ?
                                computeWeightedSimilarity(
                                    vector1, vectors.get(node2), weights.get(node1), weights.get(node2)
                                )
                                : computeSimilarity(vector1, vectors.get(node2));
                            if (!Double.isNaN(similarity)) {
                                topKMap.put(node1, node2, similarity);
                                topKMap.put(node2, node1, similarity);
                            }
                        });
                } else {
                    targetNodesStream()
                        .filter(node2 -> node1 != node2)
                        .forEach(node2 -> {
                            double similarity = weighted
                                ?
                                computeWeightedSimilarity(
                                    vector1, vectors.get(node2), weights.get(node1), weights.get(node2)
                                )
                                : computeSimilarity(vector1, vectors.get(node2));
                            if (!Double.isNaN(similarity)) {
                                topKMap.put(node1, node2, similarity);
                            }
                        });
                }
            });
        progressTracker.endSubTask();
        return topKMap;
    }

    private TopKMap computeTopKMapParallel() {
        progressTracker.beginSubTask(calculateWorkload());

        Comparator<SimilarityResult> comparator = config.normalizedK() > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        TopKMap topKMap = new TopKMap(vectors.size(), sourceNodes, Math.abs(config.normalizedK()), comparator);
        ParallelUtil.parallelStreamConsume(
            loggableAndTerminatableSourceNodeStream(),
            concurrency,
            stream -> stream
                .forEach(node1 -> {
                    long[] vector1 = vectors.get(node1);
                    // We deliberately compute the full matrix (except the diagonal).
                    // The parallel workload is partitioned based on the outer stream.
                    // The TopKMap stores a priority queue for each node. Writing
                    // into these queues is not considered to be thread-safe.
                    // Hence, we need to ensure that down the stream, exactly one queue
                    // within the TopKMap processes all pairs for a single node.
                    targetNodesStream()
                        .filter(node2 -> node1 != node2)
                        .forEach(node2 -> {
                            double similarity = weighted
                                ?
                                computeWeightedSimilarity(
                                    vector1, vectors.get(node2), weights.get(node1), weights.get(node2)
                                )
                                : computeSimilarity(vector1, vectors.get(node2));
                            if (!Double.isNaN(similarity)) {
                                topKMap.put(node1, node2, similarity);
                            }
                        });
                })
        );

        progressTracker.endSubTask();
        return topKMap;
    }

    private Stream<SimilarityResult> computeTopN() {
        progressTracker.beginSubTask(calculateWorkload());

        TopNList topNList = new TopNList(config.normalizedN());
        loggableAndTerminatableSourceNodeStream()
            .forEach(node1 -> {
                long[] vector1 = vectors.get(node1);

                if (sourceNodeFilter.equals(NodeFilter.noOp)) {
                    targetNodesStream(node1 + 1)
                        .forEach(node2 -> {
                            double similarity = weighted
                                ?
                                computeWeightedSimilarity(
                                    vector1, vectors.get(node2), weights.get(node1), weights.get(node2)
                                )
                                : computeSimilarity(vector1, vectors.get(node2));
                            if (!Double.isNaN(similarity)) {
                                topNList.add(node1, node2, similarity);
                            }
                        });
                } else {
                    targetNodesStream()
                        .filter(node2 -> node1 != node2)
                        .forEach(node2 -> {
                            double similarity = weighted
                                ?
                                computeWeightedSimilarity(
                                    vector1, vectors.get(node2), weights.get(node1), weights.get(node2)
                                )
                                : computeSimilarity(vector1, vectors.get(node2));
                            if (!Double.isNaN(similarity)) {
                                topNList.add(node1, node2, similarity);
                            }
                        });
                }

            });

        progressTracker.endSubTask();
        return topNList.stream();
    }

    private Stream<SimilarityResult> computeTopN(TopKMap topKMap) {
        TopNList topNList = new TopNList(config.normalizedN());
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

    private double computeWeightedSimilarity(long[] vector1, long[] vector2, double[] weights1, double[] weights2) {
        double similarity = similarityComputer.computeWeightedSimilarity(vector1, vector2, weights1, weights2);
        progressTracker.logProgress();
        return similarity;
    }

    private double computeSimilarity(long[] vector1, long[] vector2) {
        double similarity = similarityComputer.computeSimilarity(vector1, vector2);
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
        boolean isNotFiltered = sourceNodes.equals(NodeFilter.noOp) && targetNodeFilter.equals(NodeFilter.noOp);
        if (concurrency == 1 && isNotFiltered) {
            workload = workload / 2;
        }
        return workload;
    }

    private Stream<SimilarityResult> computeSimilaritiesForNode(long node1) {
        long[] vector1 = vectors.get(node1);
        return targetNodesStream(node1 + 1)
            .mapToObj(node2 -> {
                double similarity = weighted
                    ? computeWeightedSimilarity(vector1, vectors.get(node2), weights.get(node1), weights.get(node2))
                    : computeSimilarity(vector1, vectors.get(node2));
                return Double.isNaN(similarity) ? null : new SimilarityResult(
                    node1,
                    node2,
                    similarity
                );
            })
            .filter(Objects::nonNull);
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
