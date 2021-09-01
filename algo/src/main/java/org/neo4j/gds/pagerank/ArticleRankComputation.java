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
package org.neo4j.gds.pagerank;

import com.carrotsearch.hppc.LongSet;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.Messages;
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.beta.pregel.Reducer;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;

import java.util.Optional;
import java.util.function.LongToDoubleFunction;

public final class ArticleRankComputation implements PregelComputation<PageRankConfig> {

    static final String PAGE_RANK = "pagerank";

    private final boolean hasSourceNodes;
    private final LongSet sourceNodes;
    private final LongToDoubleFunction degreeFunction;

    private final double dampingFactor;
    private final double tolerance;
    private final double alpha;
    private final double averageDegree;

    ArticleRankComputation(
        PageRankConfig config,
        LongSet sourceNodes,
        LongToDoubleFunction degreeFunction,
        double averageDegree
    ) {
        this.dampingFactor = config.dampingFactor();
        this.tolerance = config.tolerance();
        this.averageDegree = averageDegree;
        this.alpha = 1 - this.dampingFactor;
        this.sourceNodes = sourceNodes;
        this.hasSourceNodes = !sourceNodes.isEmpty();
        this.degreeFunction = degreeFunction;
    }

    @Override
    public PregelSchema schema(PageRankConfig config) {
        return new PregelSchema.Builder().add(PAGE_RANK, ValueType.DOUBLE).build();
    }

    @Override
    public void init(InitContext<PageRankConfig> context) {
        context.setNodeValue(PAGE_RANK, initialValue(context));
    }

    private double initialValue(InitContext<PageRankConfig> context) {
        if (!hasSourceNodes || sourceNodes.contains(context.nodeId())) {
            return alpha;
        }
        return 0;
    }

    @Override
    public void compute(ComputeContext<PageRankConfig> context, Messages messages) {
        double rank = context.doubleNodeValue(PAGE_RANK);
        double delta = rank;

        if (!context.isInitialSuperstep()) {
            double sum = 0;
            for (var message : messages) {
                sum += message;
            }
            delta = dampingFactor * sum;
            context.setNodeValue(PAGE_RANK, rank + delta);
        }

        if (delta > tolerance || context.isInitialSuperstep()) {
            var degree = degreeFunction.applyAsDouble(context.nodeId());
            if (degree > 0) {
                // different from the original ArticleRank paper as we use deltas instead of the whole rank
                // to avoid exploding scores, we use `1 / (degree + avgDegree)`
                // instead of the proposed `avgDegree / (degree + avgDegree)`
                context.sendToNeighbors(delta / (degree + averageDegree));
            }
        } else {
            context.voteToHalt();
        }
    }

    @Override
    public Optional<Reducer> reducer() {
        return Optional.of(new Reducer.Sum());
    }

    @Override
    public double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
        return nodeValue * relationshipWeight;
    }

}
