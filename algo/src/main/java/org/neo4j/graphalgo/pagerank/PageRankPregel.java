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
package org.neo4j.graphalgo.pagerank;

import com.carrotsearch.hppc.LongScatterSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.Messages;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelSchema;
import org.neo4j.graphalgo.beta.pregel.Reducer;
import org.neo4j.graphalgo.beta.pregel.context.ComputeContext;
import org.neo4j.graphalgo.beta.pregel.context.InitContext;

import java.util.Optional;

public class PageRankPregel implements PregelComputation<PageRankPregelConfig> {

    static final String PAGE_RANK = "pagerank";

    private final boolean weighted;
    private final String seedProperty;
    private final LongSet sourceNodes;

    private final double dampingFactor;
    private final double tolerance;
    private final double alpha;

    public PageRankPregel(PageRankPregelConfig config) {
        this.weighted = config.relationshipWeightProperty() != null;
        this.seedProperty = config.seedProperty();
        this.dampingFactor = config.dampingFactor();
        this.tolerance = config.tolerance();
        this.alpha = 1 - this.dampingFactor;
        this.sourceNodes = new LongScatterSet();
        config.sourceNodeIds().forEach(sourceNodes::add);
    }

    @Override
    public PregelSchema schema(PageRankPregelConfig config) {
        return new PregelSchema.Builder().add(PAGE_RANK, ValueType.DOUBLE).build();
    }

    @Override
    public void init(InitContext<PageRankPregelConfig> context) {
        var nodeId = context.nodeId();
        var initialValue = seedProperty != null
            ? context.nodeProperties(seedProperty).doubleValue(nodeId)
            : sourceNodes.contains(nodeId) ? alpha : 0;
        context.setNodeValue(PAGE_RANK, initialValue);
    }

    @Override
    public void compute(ComputeContext<PageRankPregelConfig> context, Messages messages) {
        double newRank = context.doubleNodeValue(PAGE_RANK);

        double delta = newRank;

        // compute new rank based on neighbor ranks
        if (!context.isInitialSuperstep()) {
            double sum = 0;
            for (var message : messages) {
                sum += message;
            }

            delta = dampingFactor * sum;
            context.setNodeValue(PAGE_RANK, newRank + delta);
        }

        if (delta > tolerance || context.isInitialSuperstep()) {
            // send new rank to neighbors
            if (weighted) {
                // normalized via `applyRelationshipWeight`
                context.sendToNeighbors(newRank);
            } else {
                int degree = context.degree();
                context.sendToNeighbors(delta / degree);
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
        // ! assuming normalized relationshipWeights (sum of outgoing edge weights = 1 and none negative weights)
        return nodeValue * relationshipWeight;
    }

}
