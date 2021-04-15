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
import org.neo4j.graphalgo.beta.pregel.Partitioning;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.SourceNodesConfig;
import org.neo4j.graphalgo.config.ToleranceConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Optional;

@ValueClass
@Configuration("PageRankPregelConfigImpl")
@SuppressWarnings("immutables:subtype")
public interface PageRankPregelConfig extends
    PregelConfig,
    ToleranceConfig,
    SourceNodesConfig
{
    @Value.Default
    @Override
    default double tolerance() {
        return 1E-7;
    }

    @Value.Default
    @Override
    default int maxIterations() {
        return 20;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0, max = 1, maxInclusive = false)
    default double dampingFactor() {
        return 0.85;
    }

    @Deprecated
    @Value.Default
    default boolean cacheWeights() {
        return false;
    }

    @Override
    @Value.Default
    @Configuration.Ignore
    default boolean isAsynchronous() {
        return false;
    }

    @Override
    @Value.Default
    @Configuration.Ignore
    default Partitioning partitioning() {
        return Partitioning.RANGE;
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
