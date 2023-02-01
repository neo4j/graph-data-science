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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

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

    public UndirectedEdgeSplitter(
        Optional<Long> maybeSeed,
        IdMap sourceNodes,
        IdMap targetNodes,
        RelationshipType selectedRelationshipType,
        RelationshipType remainingRelationshipType,
        int concurrency
    ) {
        super(maybeSeed, sourceNodes, targetNodes, selectedRelationshipType, remainingRelationshipType, concurrency);
    }

    @Override
    protected long validPositiveRelationshipCandidateCount(Graph graph, LongLongPredicate isValidNodePair) {
        var validRelationshipCountAdder = new LongAdder();

        var countValidRelationshipTasks = PartitionUtils.rangePartition(concurrency,
            graph.nodeCount(),
            partition -> (Runnable) () -> {
                var concurrentGraph = graph.concurrentCopy();
                partition.consume(nodeId -> concurrentGraph.forEachRelationship(nodeId, (s, t) -> {
                    if (s < t) {
                        //If one directed edge has valid labels, then increment count by 2 for undirected graph to get correct total positiveSamples count.
                        //because we only do directed positiveSampling, which is choosing one of the positive undirected relationship.
                        //Otherwise, if sourceNodeLabels != targetNodeLabels, positiveSamples will be too small.
                        if (isValidNodePair.apply(s, t) || isValidNodePair.apply(t, s)) {
                            validRelationshipCountAdder.add(2);
                        }
                    }
                    return true;
                }));
            }, Optional.empty()
        );

        RunWithConcurrency.builder().concurrency(concurrency).tasks(countValidRelationshipTasks).run();

        return validRelationshipCountAdder.longValue();
    }

    @Override
    protected void positiveSampling(
        Graph graph,
        RelationshipsBuilder selectedRelsBuilder,
        RelationshipWithPropertyConsumer remainingRelsConsumer,
        MutableLong selectedRelCount,
        MutableLong remainingRelCount,
        long nodeId,
        LongLongPredicate isValidNodePair,
        MutableLong positiveSamplesRemaining,
        MutableLong candidateEdgesRemaining
    ) {
        graph.forEachRelationship(nodeId, Double.NaN, (source, target, weight) -> {
            if (source < target) {
                // we handle also reverse edge here
                // the effect of self-loops are disregarded
                if (isValidNodePair.apply(source, target) || isValidNodePair.apply(target, source)) {
                    if (sample(positiveSamplesRemaining.doubleValue() / candidateEdgesRemaining.doubleValue())) {
                        positiveSamplesRemaining.addAndGet(-2);
                        selectedRelCount.increment();
                        if (isValidNodePair.apply(source, target)) {
                            selectedRelsBuilder.addFromInternal(graph.toRootNodeId(source), graph.toRootNodeId(target), POSITIVE);
                        } else {
                            selectedRelsBuilder.addFromInternal(graph.toRootNodeId(target), graph.toRootNodeId(source), POSITIVE);
                        }
                    } else {
                        remainingRelCount.increment();
                        remainingRelsConsumer.accept(source, target, weight);
                    }
                    // because of reverse edge
                    candidateEdgesRemaining.addAndGet(-2);
                }
                // invalid relationships will be added to neither holdout or remaining
            }
            return true;
        });
    }
}
