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
package org.neo4j.gds.projection;

import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.core.loading.GraphProjectResult;

import java.util.Map;

public class GraphProjectNativeResult extends GraphProjectResult {
    public final Map<String, Object> nodeProjection;
    public final Map<String, Object> relationshipProjection;

    public GraphProjectNativeResult(
        String graphName,
        Map<String, Object> nodeProjection,
        Map<String, Object> relationshipProjection,
        long nodeCount,
        long relationshipCount,
        long projectMillis
    ) {
        super(graphName, nodeCount, relationshipCount, projectMillis);
        this.nodeProjection = nodeProjection;
        this.relationshipProjection = relationshipProjection;
    }

    public static final class Builder extends GraphProjectResult.Builder<GraphProjectNativeResult> {
        private final NodeProjections nodeProjections;
        private final RelationshipProjections relationshipProjections;

        public Builder(GraphProjectFromStoreConfig config) {
            super(config);
            this.nodeProjections = config.nodeProjections();
            this.relationshipProjections = config.relationshipProjections();
        }

        public GraphProjectNativeResult build() {
            return new GraphProjectNativeResult(
                graphName,
                nodeProjections.toObject(),
                relationshipProjections.toObject(),
                nodeCount,
                relationshipCount,
                projectMillis
            );
        }
    }
}
