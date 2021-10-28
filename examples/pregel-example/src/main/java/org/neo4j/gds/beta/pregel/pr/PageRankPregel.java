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
package org.neo4j.gds.beta.pregel.pr;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.Messages;
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.beta.pregel.Reducer;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.beta.pregel.annotation.PregelProcedure;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.SeedConfig;

import java.util.Optional;

@PregelProcedure(name = "example.pregel.pr", modes = {GDSMode.STREAM, GDSMode.MUTATE})
public class PageRankPregel implements PregelComputation<PageRankPregel.PageRankPregelConfig> {

    static final String PAGE_RANK = "pagerank";

    private static boolean weighted;

    @Override
    public PregelSchema schema(PageRankPregelConfig config) {
        return new PregelSchema.Builder().add(PAGE_RANK, ValueType.DOUBLE).build();
    }

    @Override
    public void init(InitContext<PageRankPregelConfig> context) {
        var initialValue = context.config().seedProperty() != null
            ? context.nodeProperties(context.config().seedProperty()).doubleValue(context.nodeId())
            : 1.0 / context.nodeCount();
        context.setNodeValue(PAGE_RANK, initialValue);

        weighted = context.config().hasRelationshipWeightProperty();
    }

    @Override
    public void compute(ComputeContext<PageRankPregelConfig> context, Messages messages) {
        double newRank = context.doubleNodeValue(PAGE_RANK);

        // compute new rank based on neighbor ranks
        if (!context.isInitialSuperstep()) {
            double sum = 0;
            for (var message : messages) {
                sum += message;
            }

            var dampingFactor = context.config().dampingFactor();
            var jumpProbability = 1 - dampingFactor;

            newRank = (jumpProbability / context.nodeCount()) + dampingFactor * sum;

            context.setNodeValue(PAGE_RANK, newRank);
        }

        // send new rank to neighbors
        if (weighted) {
            // normalized via `applyRelationshipWeight`
            context.sendToNeighbors(newRank);
        } else {
            context.sendToNeighbors(newRank / context.degree());
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
    public interface PageRankPregelConfig extends PregelProcedureConfig, SeedConfig {
        @Value.Default
        default double dampingFactor() {
            return 0.85;
        }

        static PageRankPregelConfig of(
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper userInput
        ) {
            return new PageRankPregelConfigImpl(graphName, maybeImplicitCreate, userInput);
        }
    }
}
