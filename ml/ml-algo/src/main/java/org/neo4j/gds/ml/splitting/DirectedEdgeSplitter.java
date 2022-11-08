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
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public class DirectedEdgeSplitter extends EdgeSplitter {

    public DirectedEdgeSplitter(
        Optional<Long> maybeSeed,
        IdMap sourceLabels,
        IdMap targetLabels,
        int concurrency
    ) {
        super(maybeSeed, sourceLabels, targetLabels, concurrency);
    }

    private long validPositiveRelationshipCandidateCount(
        Graph graph,
        LongLongPredicate isValidNodePair,
        HugeLongArray degreeResult
    ) {
        LongAdder validRelationshipCountAdder = new LongAdder();
        var countValidRelationshipTasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> (Runnable) () -> {
                var concurrentGraph = graph.concurrentCopy();
                partition.consume(nodeId -> concurrentGraph.forEachRelationship(nodeId, (s, t) -> {
                        if (isValidNodePair.apply(s, t)) {
                            validRelationshipCountAdder.add(1);
                            degreeResult.addTo(nodeId, 1);
                        }
                        return true;
                    })
                );
            }, Optional.empty()
        );

        RunWithConcurrency.builder().concurrency(concurrency).tasks(countValidRelationshipTasks).run();

        return validRelationshipCountAdder.longValue();
    }

    @Override
    public SplitResult splitPositiveExamples(
        Graph graph,
        double holdoutFraction
    ) {
        LongPredicate isValidSourceNode = node -> sourceNodes.contains(graph.toOriginalNodeId(node));
        LongPredicate isValidTargetNode = node -> targetNodes.contains(graph.toOriginalNodeId(node));
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

        var validDegrees = HugeLongArray.newArray(graph.nodeCount());
        var validRelationshipCount = validPositiveRelationshipCandidateCount(graph, isValidNodePair, validDegrees);

        int positiveSamples = (int) (validRelationshipCount * holdoutFraction);
        var positiveSamplesRemaining = new MutableLong(positiveSamples);

        var candidateEdgesRemaining = new MutableLong(validRelationshipCount);

        graph.forEachNode(nodeId -> {
            positiveSampling(
                graph,
                selectedRelsBuilder,
                remainingRelsConsumer,
                positiveSamplesRemaining,
                nodeId,
                isValidSourceNode,
                isValidTargetNode,
                candidateEdgesRemaining
            );

            return true;
        });

        return SplitResult.of(
            remainingRelsBuilder,
            validRelationshipCount - positiveSamples,
            selectedRelsBuilder,
            positiveSamples
        );
    }

    private void positiveSampling(
        Graph graph,
        RelationshipsBuilder selectedRelsBuilder,
        RelationshipWithPropertyConsumer remainingRelsConsumer,
        MutableLong positiveSamplesRemaining,
        long nodeId,
        LongPredicate isValidSourceNode,
        LongPredicate isValidTargetNode,
        MutableLong candidateEdgesRemaining
    ) {
        LongLongPredicate isValidNodePair = (s, t) -> isValidSourceNode.apply(s) && isValidTargetNode.apply(t);

        graph.forEachRelationship(nodeId, Double.NaN, (source, target, weight) -> {
            if (isValidNodePair.apply(source, target)) {
                if (sample(positiveSamplesRemaining.doubleValue() / candidateEdgesRemaining.doubleValue())) {
                    positiveSamplesRemaining.decrementAndGet();
                    selectedRelsBuilder.addFromInternal(graph.toRootNodeId(source), graph.toRootNodeId(target), POSITIVE);
                } else {
                    remainingRelsConsumer.accept(source, target, weight);
                }
                candidateEdgesRemaining.addAndGet(-1);
            }
            return true;
        });
    }
}
