/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel.pr;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.NodeSchemaBuilder;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.beta.pregel.PregelContext;
import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.SeedConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Optional;

@PregelProcedure(name = "example.pregel.pr", modes = {GDSMode.STREAM, GDSMode.MUTATE})
public class PageRankPregel implements PregelComputation<PageRankPregel.PageRankPregelConfig> {

    static final String PAGE_RANK = "pagerank";

    @Override
    public Pregel.NodeSchema nodeSchema() {
        return new NodeSchemaBuilder()
            .putElement(PAGE_RANK, ValueType.DOUBLE)
            .build();
    }

    @Override
    public void init(PregelContext.InitContext<PageRankPregelConfig> context) {
        var initialValue = context.getConfig().seedProperty() != null
            ? context.nodeProperties(context.getConfig().seedProperty()).doubleValue(context.nodeId())
            : 1.0 / context.getNodeCount();
        context.setNodeValue(PAGE_RANK, initialValue);
    }

    @Override
    public void compute(PregelContext.ComputeContext<PageRankPregelConfig> context, Pregel.Messages messages) {
        double newRank = context.doubleNodeValue(PAGE_RANK);

        // compute new rank based on neighbor ranks
        if (!context.isInitialSuperstep()) {
            double sum = 0;
            for (var message : messages) {
                sum += message;
            }

            var dampingFactor = context.getConfig().dampingFactor();
            var jumpProbability = 1 - dampingFactor;

            newRank = (jumpProbability / context.getNodeCount()) + dampingFactor * sum;

            context.setNodeValue(PAGE_RANK, newRank);
        }

        // send new rank to neighbors
        context.sendToNeighbors(newRank / context.getDegree());
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
