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


/**
 * Splits an undirected graph into two Relationships objects.
 * The first represents a holdout set and is a directed graph.
 * The second represents the remaining graph and is also undirected.
 * For each held out undirected edge, the holdout set is populated with
 * an edge with the same underlying node pair but with random direction.
 * The holdout fraction is denominated in fraction of undirected edges.
 */
public class UndirectedEdgeSplitter extends EdgeSplitter {

    public UndirectedEdgeSplitter(Optional<Long> maybeSeed, double negativeSamplingRatio, Collection<NodeLabel> sourceLabels, Collection<NodeLabel> targetLabels, int concurrency) {
        super(maybeSeed, negativeSamplingRatio, sourceLabels, targetLabels, concurrency);
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
        // TODO: move this validation higher into the hierarchy
        if (!graph.isUndirected()) {
            throw new IllegalArgumentException("EdgeSplitter requires graph to be UNDIRECTED");
        }
        if (!masterGraph.isUndirected()) {
            throw new IllegalArgumentException("EdgeSplitter requires master graph to be UNDIRECTED");
        }

        var sourceNodesWithRequiredNodeLabels = graph.withFilteredLabels(sourceLabels, concurrency);
        var targetNodesWithRequiredNodeLabels = graph.withFilteredLabels(targetLabels, concurrency);
        LongLongPredicate isValidNodePair = (s, t) -> sourceNodesWithRequiredNodeLabels.contains(s) && targetNodesWithRequiredNodeLabels.contains(t);

        RelationshipsBuilder selectedRelsBuilder = newRelationshipsBuilderWithProp(graph, Orientation.NATURAL);

        RelationshipsBuilder remainingRelsBuilder = graph.hasRelationshipProperty()
            ? newRelationshipsBuilderWithProp(graph, Orientation.UNDIRECTED)
            : newRelationshipsBuilder(graph, Orientation.UNDIRECTED);
        RelationshipWithPropertyConsumer remainingRelsConsumer = graph.hasRelationshipProperty()
            ? (s, t, w) -> { remainingRelsBuilder.addFromInternal(graph.toRootNodeId(s), graph.toRootNodeId(t), w); return true; }
            : (s, t, w) -> { remainingRelsBuilder.addFromInternal(graph.toRootNodeId(s), graph.toRootNodeId(t)); return true; };

        LongAdder validRelationshipCountAdder = new LongAdder();
        var countValidRelationshipTasks = PartitionUtils.rangePartition(concurrency, graph.nodeCount(), partition -> (Runnable)()->{
            var concurrentGraph = graph.concurrentCopy();
            partition.consume(nodeId -> {
                if (sourceNodesWithRequiredNodeLabels.contains(nodeId)) {
                    concurrentGraph.forEachRelationship(nodeId, (s, t) ->{
                        if (targetNodesWithRequiredNodeLabels.contains(t)) validRelationshipCountAdder.increment();
                        return true;
                    });
                }
            });
        }, Optional.empty());

        RunWithConcurrency.builder().concurrency(concurrency).tasks(countValidRelationshipTasks).run();

        var validRelationshipCount = validRelationshipCountAdder.longValue();

        var positiveSamples = (long) (validRelationshipCount * holdoutFraction) / 2;
        var positiveSamplesRemaining = new MutableLong(positiveSamples);
        var negativeSamples = (long) (negativeSamplingRatio * validRelationshipCount * holdoutFraction) / 2;
        var negativeSamplesRemaining = new MutableLong(negativeSamples);
        var edgesRemaining = new MutableLong(validRelationshipCount);

        graph.forEachNode(nodeId -> {
            positiveSampling(
                graph,
                selectedRelsBuilder,
                remainingRelsConsumer,
                positiveSamplesRemaining,
                edgesRemaining,
                nodeId,
                isValidNodePair
            );

            negativeSampling(
                graph,
                masterGraph,
                selectedRelsBuilder,
                negativeSamplesRemaining,
                nodeId
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
        MutableLong edgesRemaining,
        long nodeId,
        LongLongPredicate isValidNodePair
    ) {
        graph.forEachRelationship(nodeId, Double.NaN, (source, target, weight) -> {
            if (source == target) {
                edgesRemaining.decrementAndGet();
            }
            if (source < target && (isValidNodePair.apply(source, target) || isValidNodePair.apply(target, source))) {
                // we handle also reverse edge here
                // the effect of self-loops are disregarded
                if (sample(2 * positiveSamplesRemaining.doubleValue() / edgesRemaining.doubleValue())) {
                    positiveSamplesRemaining.decrementAndGet();
                    if (sample(0.5)) {
                        selectedRelsBuilder.addFromInternal(graph.toRootNodeId(source), graph.toRootNodeId(target), POSITIVE);
                    } else {
                        selectedRelsBuilder.addFromInternal(graph.toRootNodeId(target), graph.toRootNodeId(source), POSITIVE);
                    }
                } else {
                    remainingRelsConsumer.accept(source, target, weight);
                }
                // because of reverse edge
                edgesRemaining.addAndGet(-2);
            }
            return true;
        });
    }
}
