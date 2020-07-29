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
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.beta.pregel.PregelContext;
import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Optional;
import java.util.Queue;

@PregelProcedure(name = "example.pregel.pr", modes = {GDSMode.STREAM})
public class PageRankPregel implements PregelComputation<PageRankPregel.PageRankPregelConfig> {

    @Override
    public void compute(PregelContext<PageRankPregel.PageRankPregelConfig> pregel, long nodeId, Queue<Double> messages) {
        if (pregel.isInitialSuperstep()) {
            pregel.setNodeValue(nodeId, 1.0 / pregel.getNodeCount());
        }

        double newRank = pregel.getNodeValue(nodeId);

        // compute new rank based on neighbor ranks
        if (!pregel.isInitialSuperstep()) {
            double sum = 0;
            if (messages != null) {
                Double nextMessage;
                while (!(nextMessage = messages.poll()).isNaN()) {
                    sum += nextMessage;
                }
            }

            var dampingFactor = pregel.getConfig().dampingFactor();
            var jumpProbability = 1 - dampingFactor;

            newRank = (jumpProbability / pregel.getNodeCount()) + dampingFactor * sum;
        }

        // send new rank to neighbors
        pregel.setNodeValue(nodeId, newRank);
        pregel.sendMessages(nodeId, newRank / pregel.getDegree(nodeId));
    }

    @ValueClass
    @Configuration("PageRankPregelConfigImpl")
    @SuppressWarnings("immutables:subtype")
    interface PageRankPregelConfig extends PregelConfig {
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
