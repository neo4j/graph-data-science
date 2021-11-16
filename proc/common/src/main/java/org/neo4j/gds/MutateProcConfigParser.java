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

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Optional;

import static org.neo4j.gds.config.GraphCreateConfig.NODE_COUNT_KEY;
import static org.neo4j.gds.config.GraphCreateConfig.RELATIONSHIP_COUNT_KEY;

public class MutateProcConfigParser<CONFIG extends AlgoBaseConfig> extends ProcConfigParser<CONFIG> {

    private final ProcConfigParser<CONFIG> defaultParser;

    public MutateProcConfigParser(ProcConfigParser<CONFIG> defaultParser) {
        super(defaultParser.username);
        this.defaultParser = defaultParser;
    }

    @Override
    protected CONFIG newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return defaultParser.newConfig(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    public CONFIG newConfig(Optional<String> graphName, CypherMapWrapper config) {
        if (graphName.isEmpty() && !(config.containsKey(NODE_COUNT_KEY) || config.containsKey(RELATIONSHIP_COUNT_KEY))) {
            throw new IllegalArgumentException(
                "Cannot mutate implicitly loaded graphs. Use a loaded graph in the graph-catalog"
            );
        }
        return super.newConfig(graphName, config);
    }
}
