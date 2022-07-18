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
package org.neo4j.gds.ml.splitting;

import com.carrotsearch.hppc.predicates.LongLongPredicate;
import com.carrotsearch.hppc.predicates.LongPredicate;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public class DirectedEdgeSplitter extends EdgeSplitter {

    public DirectedEdgeSplitter(
        Optional<Long> maybeSeed,
        double negativeSamplingRatio,
        Collection<NodeLabel> sourceLabels,
        Collection<NodeLabel> targetLabels,
        int concurrency
    ) {
        super(maybeSeed, negativeSamplingRatio, sourceLabels, targetLabels, concurrency);
    }

    @Override
    long validPositiveRelationshipCandidateCount(Graph graph, LongLongPredicate isValidNodePair) {
        LongAdder validRelationshipCountAdder = new LongAdder();
        var countValidRelationshipTasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> (Runnable) () -> {
                var concurrentGraph = graph.concurrentCopy();
                partition.consume(nodeId -> concurrentGraph.forEachRelationship(nodeId, (s, t) -> {
                    if (isValidNodePair.apply(s, t)) {
                        validRelationshipCountAdder.add(1);
                    }
                    return true;
                }));
            }, Optional.empty()
        );

        RunWithConcurrency.builder().concurrency(concurrency).tasks(countValidRelationshipTasks).run();

        return validRelationshipCountAdder.longValue();
    }

    @TestOnly
    public SplitResult split(Graph graph, double holdoutFraction) {
        return split(graph, graph, holdoutFraction);
    }

    @Override
    public SplitResult split(
        Graph graph,
        Graph masterGraph,
        double holdoutFraction
    ) {
        var sourceNodes = sourceNodes(graph);
        MutableLong positiveSourceNodeCount = new MutableLong(sourceNodes.nodeCount());
        MutableLong negativeSourceNodeCount = new MutableLong(positiveSourceNodeCount.longValue());
        var targetNodes = targetNodes(graph);

        LongPredicate isValidSourceNode = node -> sourceNodes.contains(graph.toRootNodeId(node));
        LongPredicate isValidTargetNode = node -> targetNodes.contains(graph.toRootNodeId(node));
        LongLongPredicate isValidNodePair = (s, t) -> isValidSourceNode.apply(s) && isValidTargetNode.apply(t);

        RelationshipsBuilder selectedRelsBuilder = newRelationshipsBuilderWithProp(graph, Orientation.NATURAL);

        RelationshipsBuilder remainingRelsBuilder;
        RelationshipWithPropertyConsumer remainingRelsConsumer;
        if (graph.hasRelationshipProperty()) {
            remainingRelsBuilder = newRelationshipsBuilderWithProp(graph, Orientation.NATURAL);
            remainingRelsConsumer = (s, t, w) -> {
                remainingRelsBuilder.addFromInternal(graph.toRootNodeId(s), graph.toRootNodeId(t), w);
                return true;
            };
        } else {
            remainingRelsBuilder = newRelationshipsBuilder(graph, Orientation.NATURAL);
            remainingRelsConsumer = (s, t, w) -> {
                remainingRelsBuilder.addFromInternal(graph.toRootNodeId(s), graph.toRootNodeId(t));
                return true;
            };
        }

        var validRelationshipCount = validPositiveRelationshipCandidateCount(graph, isValidNodePair);

        int positiveSamples = (int) (validRelationshipCount * holdoutFraction);
        var positiveSamplesRemaining = new MutableLong(positiveSamples);
        var negativeSamples = (long) (negativeSamplingRatio * validRelationshipCount * holdoutFraction);
        var negativeSamplesRemaining = new MutableLong(negativeSamples);

        graph.forEachNode(nodeId -> {
            positiveSampling(
                graph,
                selectedRelsBuilder,
                remainingRelsConsumer,
                positiveSamplesRemaining,
                nodeId,
                positiveSourceNodeCount,
                isValidSourceNode,
                isValidTargetNode
            );

            negativeSampling(
                graph,
                masterGraph,
                selectedRelsBuilder,
                negativeSamplesRemaining,
                nodeId,
                isValidSourceNode,
                isValidTargetNode,
                negativeSourceNodeCount
            );
            return true;
        });

        return SplitResult.of(remainingRelsBuilder.build(), selectedRelsBuilder.build());
    }

    private void positiveSampling(
        Graph graph,
        RelationshipsBuilder selectedRelsBuilder,
        RelationshipWithPropertyConsumer remainingRelsConsumer,
        MutableLong positiveSamplesRemaining,
        long nodeId,
        MutableLong validSourceNodeCount,
        LongPredicate isValidSourceNode,
        LongPredicate isValidTargetNode
    ) {
        if (!isValidSourceNode.apply(nodeId)) {
            graph.forEachRelationship(nodeId, Double.NaN, remainingRelsConsumer);
            return;
        }

        MutableLong validDegree = new MutableLong();
        graph.forEachRelationship(nodeId, (source, target) -> {
            if (isValidTargetNode.apply(target)) {
                validDegree.increment();
            }
            return true;
        });

        var relsToSelectFromThisNode = samplesPerNode(
            validDegree.longValue(),
            positiveSamplesRemaining.doubleValue(),
            validSourceNodeCount.getAndDecrement()
        );
        var localSelectedRemaining = new MutableLong(relsToSelectFromThisNode);

        graph.forEachRelationship(nodeId, Double.NaN, (source, target, weight) -> {
            double localSelectedDouble = localSelectedRemaining.doubleValue();
            if (isValidTargetNode.apply(target)) {
                var isSelected = sample(localSelectedDouble / validDegree.getAndDecrement());
                if (relsToSelectFromThisNode > 0 && localSelectedDouble > 0 && isSelected) {
                    positiveSamplesRemaining.decrementAndGet();
                    localSelectedRemaining.decrementAndGet();
                    selectedRelsBuilder.addFromInternal(graph.toRootNodeId(source), graph.toRootNodeId(target), POSITIVE);
                } else {
                    remainingRelsConsumer.accept(source, target, weight);
                }
            }
            else {
                remainingRelsConsumer.accept(source, target, weight);
            }
            return true;
        });
    }
}
