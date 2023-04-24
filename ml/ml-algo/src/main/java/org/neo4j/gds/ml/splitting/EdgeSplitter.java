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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import java.util.List;
import java.util.Optional;
import java.util.Random;

public abstract class EdgeSplitter {
    public static final double POSITIVE = 1D;
    public static final String RELATIONSHIP_PROPERTY = "label";
    private final Random rng;
    private final RelationshipType selectedRelationshipType;
    private final RelationshipType remainingRelationshipType;

    protected final IdMap sourceNodes;
    protected final IdMap targetNodes;

    protected int concurrency;

    EdgeSplitter(
        Optional<Long> maybeSeed,
        IdMap sourceNodes,
        IdMap targetNodes,
        RelationshipType selectedRelationshipType,
        RelationshipType remainingRelationshipType,
        int concurrency
    ) {
        this.selectedRelationshipType = selectedRelationshipType;
        this.remainingRelationshipType = remainingRelationshipType;
        this.rng = new Random();
        maybeSeed.ifPresent(rng::setSeed);

        this.sourceNodes = sourceNodes;
        this.targetNodes = targetNodes;
        this.concurrency = concurrency;
    }

    public SplitResult splitPositiveExamples(
        Graph graph,
        double holdoutFraction,
        Optional<String> remainingRelPropertyKey
    ) {
        LongPredicate isValidSourceNode = node -> sourceNodes.containsOriginalId(graph.toOriginalNodeId(node));
        LongPredicate isValidTargetNode = node -> targetNodes.containsOriginalId(graph.toOriginalNodeId(node));
        LongLongPredicate isValidNodePair = (s, t) -> isValidSourceNode.apply(s) && isValidTargetNode.apply(t);

        RelationshipsBuilder selectedRelsBuilder = newRelationshipsBuilder(
            graph,
            selectedRelationshipType,
            Direction.DIRECTED,
            Optional.of(EdgeSplitter.RELATIONSHIP_PROPERTY)
        );

        Direction remainingRelDirection = graph.schema().direction();

        RelationshipsBuilder remainingRelsBuilder;
        RelationshipWithPropertyConsumer remainingRelsConsumer;

        remainingRelsBuilder = newRelationshipsBuilder(graph, remainingRelationshipType, remainingRelDirection, remainingRelPropertyKey);
        remainingRelsConsumer = (s, t, w) -> {
            remainingRelsBuilder.addFromInternal(graph.toRootNodeId(s), graph.toRootNodeId(t), w);
            return true;
        };

        var validRelationshipCount = validPositiveRelationshipCandidateCount(graph, isValidNodePair);

        var positiveSamples = (long) (validRelationshipCount * holdoutFraction);
        var positiveSamplesRemaining = new MutableLong(positiveSamples);
        var candidateEdgesRemaining = new MutableLong(validRelationshipCount);
        var selectedRelCount = new MutableLong(0);
        var remainingRelCount = new MutableLong(0);

        graph.forEachNode(nodeId -> {
            positiveSampling(
                graph,
                selectedRelsBuilder,
                remainingRelsConsumer,
                selectedRelCount,
                remainingRelCount,
                nodeId,
                isValidNodePair,
                positiveSamplesRemaining,
                candidateEdgesRemaining
            );

            return true;
        });

        return SplitResult.of(
            remainingRelsBuilder,
            remainingRelCount.longValue(),
            selectedRelsBuilder,
            selectedRelCount.longValue()
        );
    }

    protected abstract void positiveSampling(
        Graph graph,
        RelationshipsBuilder selectedRelsBuilder,
        RelationshipWithPropertyConsumer remainingRelsConsumer,
        MutableLong selectedRelCount,
        MutableLong remainingRelCount,
        long nodeId,
        LongLongPredicate isValidNodePair,
        MutableLong positiveSamplesRemaining,
        MutableLong candidateEdgesRemaining
    );

    protected abstract long validPositiveRelationshipCandidateCount(Graph graph, LongLongPredicate isValidNodePair);

    protected boolean sample(double probability) {
        return rng.nextDouble() < probability;
    }

    protected long samplesPerNode(long maxSamples, double remainingSamples, long remainingNodes) {
        var numSamplesOnAverage = remainingSamples / remainingNodes;
        var wholeSamples = (long) numSamplesOnAverage;
        var extraSample = sample(numSamplesOnAverage - wholeSamples) ? 1 : 0;
        return Math.min(maxSamples, wholeSamples + extraSample);
    }

    private static RelationshipsBuilder newRelationshipsBuilder(
        Graph graph,
        RelationshipType relationshipType,
        Direction direction,
        Optional<String> propertyKey
    ) {
        return GraphFactory.initRelationshipsBuilder()
            .relationshipType(relationshipType)
            .aggregation(Aggregation.SINGLE)
            .nodes(graph)
            .orientation(direction.toOrientation())
            .addAllPropertyConfigs(propertyKey
                .map(key -> List.of(GraphFactory.PropertyConfig.of(key, Aggregation.SINGLE, DefaultValue.forDouble())))
                .orElse(List.of()))
            .concurrency(1)
            .executorService(Pools.DEFAULT)
            .build();
    }

    @ValueClass
    public interface SplitResult {
        RelationshipsBuilder remainingRels();

        long remainingRelCount();
        RelationshipsBuilder selectedRels();
        long selectedRelCount();

        static EdgeSplitter.SplitResult of(
            RelationshipsBuilder remainingRels,
            long remainingRelCount,
            RelationshipsBuilder selectedRels,
            long selectedRelCount
        ) {
            return ImmutableSplitResult.of(remainingRels, remainingRelCount, selectedRels, selectedRelCount);
        }
    }
}
