/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.impl.nodesim;

import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;

import java.util.Optional;

@ValueClass
@Configuration("NodeSimilarityStreamConfigImpl")
public interface NodeSimilarityStreamConfig extends NodeSimilarityConfigBase {

    static NodeSimilarityStreamConfig of(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        NodeSimilarityStreamConfig config = new NodeSimilarityStreamConfigImpl(
            graphName,
            maybeImplicitCreate,
            username,
            userInput
        );
        config.validate();
        return config;
    }

    @Override
    @Configuration.Ignore
    default boolean computeToStream() {
        return true;
    }

    interface Builder extends NodeSimilarityConfigBase.Builder {
        @Override
        NodeSimilarityStreamConfig build();
    }
}
