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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public abstract class EdgeSplitter {

    public static final double NEGATIVE = 0D;
    public static final double POSITIVE = 1D;
    public static final String RELATIONSHIP_PROPERTY = "label";
    private static final int MAX_RETRIES = 20;

    private final Random rng;
    final double negativeSamplingRatio;

    protected final Collection<NodeLabel> sourceLabels;

    protected final Collection<NodeLabel> targetLabels;

    protected int concurrency;

    EdgeSplitter(Optional<Long> maybeSeed, double negativeSamplingRatio, Collection<NodeLabel> sourceLabels, Collection<NodeLabel> targetLabels, int concurrency) {
        this.rng = new Random();
        maybeSeed.ifPresent(rng::setSeed);

        this.negativeSamplingRatio = negativeSamplingRatio;
        this.sourceLabels = sourceLabels;
        this.targetLabels = targetLabels;
        this.concurrency = concurrency;
    }

    public abstract SplitResult split(
        Graph graph,
        Graph masterGraph,
        double holdoutFraction
    );

    protected boolean sample(double probability) {
        return rng.nextDouble() < probability;
    }

    private long randomNodeId(Graph graph) {
        return Math.abs(rng.nextLong() % graph.nodeCount());
    }

    protected long samplesPerNode(long maxSamples, double remainingSamples, long remainingNodes) {
        var numSamplesOnAverage = remainingSamples / remainingNodes;
        var wholeSamples = (long) numSamplesOnAverage;
        var extraSample = sample(numSamplesOnAverage - wholeSamples) ? 1 : 0;
        return Math.min(maxSamples, wholeSamples + extraSample);
    }

    RelationshipsBuilder newRelationshipsBuilderWithProp(Graph graph, Orientation orientation) {
        return newRelationshipsBuilder(graph, orientation, true);
    }

    RelationshipsBuilder newRelationshipsBuilder(Graph graph, Orientation orientation) {
        return newRelationshipsBuilder(graph, orientation, false);
    }

    private static RelationshipsBuilder newRelationshipsBuilder(Graph graph, Orientation orientation, boolean loadRelationshipProperty) {
        return GraphFactory.initRelationshipsBuilder()
            .aggregation(Aggregation.SINGLE)
            .nodes(graph)
            .orientation(orientation)
            .addAllPropertyConfigs(loadRelationshipProperty
                ? List.of(GraphFactory.PropertyConfig.of(Aggregation.SINGLE, DefaultValue.forDouble()))
                : List.of()
            )
            .concurrency(1)
            .executorService(Pools.DEFAULT)
            .build();
    }

    void negativeSampling(
        Graph graph,
        Graph masterGraph,
        RelationshipsBuilder selectedRelsBuilder,
        MutableLong negativeSamplesRemaining,
        long nodeId
    ) {
        var masterDegree = masterGraph.degree(nodeId);
        var negativeEdgeCount = samplesPerNode(
            (masterGraph.nodeCount() - 1) - masterDegree,
            negativeSamplesRemaining.doubleValue(),
            graph.nodeCount() - nodeId
        );

        var neighbours = new HashSet<Long>(masterDegree);
        masterGraph.forEachRelationship(nodeId, (source, target) -> {
            neighbours.add(target);
            return true;
        });

        // this will not try to avoid duplicate negative relationships,
        // nor will it avoid sampling edges that are sampled as negative in
        // an outer split.
        int retries = MAX_RETRIES;
        for (int i = 0; i < negativeEdgeCount; i++) {
            var negativeTarget = randomNodeId(graph);
            // no self-relationships
            if (!neighbours.contains(negativeTarget) && negativeTarget != nodeId) {
                negativeSamplesRemaining.decrementAndGet();
                selectedRelsBuilder.addFromInternal(graph.toRootNodeId(nodeId), graph.toRootNodeId(negativeTarget), NEGATIVE);
            } else if (retries-- > 0) {
                // we retry with a different negative target
                // skipping here and relying on finding another source node is not safe
                // we only retry a few times to protect against resampling forever for high deg nodes
                i--;
            }
        }
    }


    @ValueClass
    public interface SplitResult {
        Relationships remainingRels();
        Relationships selectedRels();

        static EdgeSplitter.SplitResult of(Relationships remainingRels, Relationships selectedRels) {
            return ImmutableSplitResult.of(remainingRels, selectedRels);
        }
    }
}
