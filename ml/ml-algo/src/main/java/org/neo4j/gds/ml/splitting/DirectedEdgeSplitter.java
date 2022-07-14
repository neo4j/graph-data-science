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
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import java.util.Collection;
import java.util.Optional;

public class DirectedEdgeSplitter extends EdgeSplitter {

    public DirectedEdgeSplitter(Optional<Long> maybeSeed, double negativeSamplingRatio, Collection<NodeLabel> sourceLabels, Collection<NodeLabel> targetLabels, int concurrency) {
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

        RelationshipsBuilder selectedRelsBuilder = newRelationshipsBuilderWithProp(graph, Orientation.NATURAL);

        RelationshipsBuilder remainingRelsBuilder = graph.hasRelationshipProperty()
            ? newRelationshipsBuilderWithProp(graph, Orientation.NATURAL)
            : newRelationshipsBuilder(graph, Orientation.NATURAL);
        RelationshipWithPropertyConsumer remainingRelsConsumer = graph.hasRelationshipProperty()
            ? (s, t, w) -> { remainingRelsBuilder.addFromInternal(graph.toRootNodeId(s), graph.toRootNodeId(t), w); return true; }
            : (s, t, w) -> { remainingRelsBuilder.addFromInternal(graph.toRootNodeId(s), graph.toRootNodeId(t)); return true; };

        int positiveSamples = (int) (graph.relationshipCount() * holdoutFraction);
        var positiveSamplesRemaining = new MutableLong(positiveSamples);
        var negativeSamples = (long) (negativeSamplingRatio * graph.relationshipCount() * holdoutFraction);
        var negativeSamplesRemaining = new MutableLong(negativeSamples);

        var sourceNodesWithRequiredNodeLabels = graph.withFilteredLabels(sourceLabels, concurrency);
        MutableLong sourceNodeCount = new MutableLong(sourceNodesWithRequiredNodeLabels.nodeCount());
        var targetNodesWithRequiredNodeLabels = graph.withFilteredLabels(targetLabels, concurrency);

        LongPredicate isValidSourceNode = sourceNodesWithRequiredNodeLabels::contains;
        LongPredicate isValidTargetNode = targetNodesWithRequiredNodeLabels::contains;
        LongLongPredicate isValidNodePair = (s, t) -> isValidSourceNode.apply(s) && isValidTargetNode.apply(t);
        graph.forEachNode(nodeId -> {
            positiveSampling(
                graph,
                selectedRelsBuilder,
                remainingRelsConsumer,
                positiveSamplesRemaining,
                nodeId
            );

            negativeSampling(
                graph,
                masterGraph,
                selectedRelsBuilder,
                negativeSamplesRemaining,
                nodeId,
                isValidSourceNode,
                isValidTargetNode,
                sourceNodeCount
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
        long nodeId
    ) {
        var degree = graph.degree(nodeId);

        var relsToSelectFromThisNode = samplesPerNode(
            degree,
            positiveSamplesRemaining.doubleValue(),
            graph.nodeCount() - nodeId
        );
        var localSelectedRemaining = new MutableLong(relsToSelectFromThisNode);
        var targetsRemaining = new MutableLong(degree);

        graph.forEachRelationship(nodeId, Double.NaN, (source, target, weight) -> {
            double localSelectedDouble = localSelectedRemaining.doubleValue();
            var isSelected = sample(localSelectedDouble / targetsRemaining.getAndDecrement());
            if (relsToSelectFromThisNode > 0 && localSelectedDouble > 0 && isSelected) {
                positiveSamplesRemaining.decrementAndGet();
                localSelectedRemaining.decrementAndGet();
                selectedRelsBuilder.addFromInternal(graph.toRootNodeId(source), graph.toRootNodeId(target), POSITIVE);
            } else {
                remainingRelsConsumer.accept(source, target, weight);
            }
            return true;
        });
    }
}
