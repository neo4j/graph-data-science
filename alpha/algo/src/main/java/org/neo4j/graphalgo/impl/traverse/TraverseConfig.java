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

package org.neo4j.graphalgo.impl.traverse;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.RelationshipWeightConfig;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@ValueClass
@Configuration("TraverseConfigImpl")
public interface TraverseConfig extends AlgoBaseConfig, RelationshipWeightConfig {

    long startNode();

    @Value.Default
    default List<Long> targetNodes() {
        return Collections.emptyList();
    }

    @Value.Default
    default long maxDepth() {
        return -1L;
    }

    @Value.Default
    default double maxCost() {
        return Double.NaN;
    }

    static TraverseConfig of(
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        String username,
        CypherMapWrapper userInput
    ) {
        return new TraverseConfigImpl(
            graphName,
            maybeImplicitCreate,
            username,
            userInput
        );
    }
}
