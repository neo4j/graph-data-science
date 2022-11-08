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

import org.neo4j.gds.Orientation;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import java.util.List;
import java.util.Optional;
import java.util.Random;

public abstract class EdgeSplitter {

    public static final double NEGATIVE = 0D;
    public static final double POSITIVE = 1D;
    public static final String RELATIONSHIP_PROPERTY = "label";
    private static final int MAX_RETRIES = 20;
    private final Random rng;

    protected final IdMap sourceNodes;
    protected final IdMap targetNodes;
    protected int concurrency;

    EdgeSplitter(Optional<Long> maybeSeed, IdMap sourceNodes, IdMap targetNodes, int concurrency) {
        this.rng = new Random();
        maybeSeed.ifPresent(rng::setSeed);

        this.sourceNodes = sourceNodes;
        this.targetNodes = targetNodes;
        this.concurrency = concurrency;
    }

    public abstract SplitResult splitPositiveExamples(
        Graph graph,
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
