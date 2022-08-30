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

import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import java.util.Optional;


/**
 * Splits an undirected graph into two Relationships objects.
 * The first represents a holdout set and is a directed graph.
 * The second represents the remaining graph and is also undirected.
 * For each held out undirected edge, the holdout set is populated with
 * an edge with the same underlying node pair but with random direction.
 * The holdout fraction is denominated in fraction of undirected edges.
 */
public class UndirectedEdgeSplitter extends EdgeSplitter {

    public UndirectedEdgeSplitter(Optional<Long> maybeSeed, double negativeSamplingRatio) {
        super(maybeSeed, negativeSamplingRatio);
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
        if (!graph.schema().isUndirected()) {
            throw new IllegalArgumentException("EdgeSplitter requires graph to be UNDIRECTED");
        }
        if (!masterGraph.schema().isUndirected()) {
            throw new IllegalArgumentException("EdgeSplitter requires master graph to be UNDIRECTED");
        }

        RelationshipsBuilder selectedRelsBuilder = newRelationshipsBuilderWithProp(graph, Orientation.NATURAL);

        RelationshipsBuilder remainingRelsBuilder = graph.hasRelationshipProperty()
            ? newRelationshipsBuilderWithProp(graph, Orientation.UNDIRECTED)
            : newRelationshipsBuilder(graph, Orientation.UNDIRECTED);
        RelationshipWithPropertyConsumer remainingRelsConsumer = graph.hasRelationshipProperty()
            ? (s, t, w) -> { remainingRelsBuilder.addFromInternal(graph.toRootNodeId(s), graph.toRootNodeId(t), w); return true; }
            : (s, t, w) -> { remainingRelsBuilder.addFromInternal(graph.toRootNodeId(s), graph.toRootNodeId(t)); return true; };

        var positiveSamples = (long) (graph.relationshipCount() * holdoutFraction) / 2;
        var positiveSamplesRemaining = new MutableLong(positiveSamples);
        var negativeSamples = (long) (negativeSamplingRatio * graph.relationshipCount() * holdoutFraction) / 2;
        var negativeSamplesRemaining = new MutableLong(negativeSamples);
        var edgesRemaining = new MutableLong(graph.relationshipCount());

        graph.forEachNode(nodeId -> {
            positiveSampling(
                graph,
                selectedRelsBuilder,
                remainingRelsConsumer,
                positiveSamplesRemaining,
                edgesRemaining,
                nodeId
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
        long nodeId
    ) {
        graph.forEachRelationship(nodeId, Double.NaN, (source, target, weight) -> {
            if (source == target) {
                edgesRemaining.decrementAndGet();
            }
            if (source < target) {
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
