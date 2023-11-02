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
package org.neo4j.gds.cypherprojection;

import org.neo4j.gds.core.loading.GraphProjectResult;

public final class GraphProjectCypherResult extends GraphProjectResult {
    public final String nodeQuery;
    public final String relationshipQuery;

    private GraphProjectCypherResult(
        String graphName,
        String nodeQuery,
        String relationshipQuery,
        long nodeCount,
        long relationshipCount,
        long projectMillis
    ) {
        super(graphName, nodeCount, relationshipCount, projectMillis);
        this.nodeQuery = nodeQuery;
        this.relationshipQuery = relationshipQuery;
    }

    public static final class Builder extends GraphProjectResult.Builder<GraphProjectCypherResult> {
        private final String nodeQuery;
        private final String relationshipQuery;

        public Builder(GraphProjectFromCypherConfig config) {
            super(config);
            this.nodeQuery = config.nodeQuery();
            this.relationshipQuery = config.relationshipQuery();
        }

        public GraphProjectCypherResult build() {
            return new GraphProjectCypherResult(
                graphName,
                nodeQuery,
                relationshipQuery,
                nodeCount,
                relationshipCount,
                projectMillis
            );
        }
    }
}
