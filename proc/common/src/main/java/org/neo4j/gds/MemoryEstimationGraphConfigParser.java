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
package org.neo4j.gds;

import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryEstimationGraphConfigParser implements ProcConfigParser<GraphCreateConfig, GraphCreateConfig> {

    private final String username;

    MemoryEstimationGraphConfigParser(String username) {
        this.username = username;
    }

    @Override
    public String username() {
        return username;
    }

    @Override
    public GraphCreateConfig processInput(Object graphNameOrConfig, Map<String, Object> configuration) {
        if (graphNameOrConfig instanceof Map) {
            var createConfigMap = (Map<String, Object>) graphNameOrConfig;
            var createConfigMapWrapper = CypherMapWrapper.create(createConfigMap);
            var graphCreateConfig = GraphCreateConfig.createImplicit(username(), createConfigMapWrapper);
            if (!graphCreateConfig.isFictitiousLoading()) {
                throw new IllegalStateException(formatWithLocale(
                    "Missing graph dimension information. Please provide node and relationship counts using the `%s` and `%s` configuration keys",
                    GraphCreateConfig.NODE_COUNT_KEY,
                    GraphCreateConfig.RELATIONSHIP_COUNT_KEY
                ));
            }
            return graphCreateConfig;
        }
        throw new IllegalArgumentException(formatWithLocale(
            "Could not parse input. Expected a configuration map, but got %s.",
            graphNameOrConfig.getClass().getSimpleName()
        ));
    }

    @Override
    public GraphCreateConfig newConfig(Optional<String> graphName, CypherMapWrapper config) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void withSharedConfigKeys(Map<String, Class<?>> sharedConfigKeys) {
        throw new UnsupportedOperationException();
    }
}
