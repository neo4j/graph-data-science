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

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.Messages;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.beta.pregel.PregelSchema;
import org.neo4j.graphalgo.beta.pregel.Reducer;
import org.neo4j.graphalgo.beta.pregel.context.ComputeContext;
import org.neo4j.graphalgo.beta.pregel.context.InitContext;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.SeedConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Optional;

public class PageRankPregel implements PregelComputation<PageRankPregel.PageRankPregelConfig> {

    static final String PAGE_RANK = "pagerank";

    private static boolean weighted;

    private final double dampingFactor;
    private final double alpha;

    public PageRankPregel(PageRankPregelConfig config) {
        this.dampingFactor = config.dampingFactor();
        this.alpha = 1 - this.dampingFactor;
    }

    @Override
    public PregelSchema schema(PageRankPregelConfig config) {
        return new PregelSchema.Builder().add(PAGE_RANK, ValueType.DOUBLE).build();
    }

    @Override
    public void init(InitContext<PageRankPregelConfig> context) {
        var initialValue = context.config().seedProperty() != null
            ? context.nodeProperties(context.config().seedProperty()).doubleValue(context.nodeId())
            : alpha;
        context.setNodeValue(PAGE_RANK, initialValue);

        weighted = context.config().relationshipWeightProperty() != null;
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

        // send new rank to neighbors
        if (weighted) {
            // normalized via `applyRelationshipWeight`
            context.sendToNeighbors(newRank);
        } else {
            int degree = context.degree();
            context.sendToNeighbors(delta / degree);
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

    @ValueClass
    @Configuration("PageRankPregelConfigImpl")
    @SuppressWarnings("immutables:subtype")
    public interface PageRankPregelConfig extends PregelConfig, SeedConfig {
        @Value.Default
        default double dampingFactor() {
            return 0.85;
        }

        static PageRankPregelConfig of(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper userInput
        ) {
            return new PageRankPregelConfigImpl(graphName, maybeImplicitCreate, username, userInput);
        }
    }
}
