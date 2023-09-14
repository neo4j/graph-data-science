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
package org.neo4j.gds.memest;

import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.projection.GraphProjectFromStoreConfig;
import org.neo4j.gds.utils.StringFormatting;

import java.util.Map;

import static org.neo4j.gds.config.GraphProjectFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.gds.projection.GraphProjectFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.gds.projection.GraphProjectFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

public class MemoryEstimationGraphConfigParser {

    private final String username;

    public MemoryEstimationGraphConfigParser(String username) {
        this.username = username;
    }

    public GraphProjectConfig parse(Object graphNameOrConfig) {
        if (graphNameOrConfig instanceof Map) {
            var createConfigMap = (Map<String, Object>) graphNameOrConfig;
            var createConfigMapWrapper = CypherMapWrapper.create(createConfigMap);
            CypherMapWrapper.PairResult result = createConfigMapWrapper.verifyMutuallyExclusivePairs(
                NODE_PROJECTION_KEY,
                RELATIONSHIP_PROJECTION_KEY,
                NODE_QUERY_KEY,
                RELATIONSHIP_QUERY_KEY,
                "Missing information for implicit graph creation."
            );
            if (result == CypherMapWrapper.PairResult.FIRST_PAIR) {
                return GraphProjectFromStoreConfig.fromProcedureConfig(username, createConfigMapWrapper);
            } else {
                return GraphProjectFromCypherConfig.fromProcedureConfig(username, createConfigMapWrapper);
            }
        }
        throw new IllegalArgumentException(StringFormatting.formatWithLocale(
            "Could not parse input. Expected a configuration map, but got %s.",
            graphNameOrConfig.getClass().getSimpleName()
        ));
    }
}
