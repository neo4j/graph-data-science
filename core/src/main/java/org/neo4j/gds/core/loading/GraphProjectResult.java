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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.config.GraphProjectConfig;

/*
 public fields because Neo4j needs to render them
 not pretty having it here in core, but with UI responsibilities
 maybe revisit one day
*/
public class GraphProjectResult {
    public final String graphName;
    public final long nodeCount;
    public final long relationshipCount;
    public final long projectMillis;

    protected GraphProjectResult(
        String graphName,
        long nodeCount,
        long relationshipCount,
        long projectMillis
    ) {
        this.graphName = graphName;
        this.nodeCount = nodeCount;
        this.relationshipCount = relationshipCount;
        this.projectMillis = projectMillis;
    }

    // protected fields because this guy is all about reuse via inheritance :grimace:
    public abstract static class Builder {
        protected final String graphName;
        protected long nodeCount;
        protected long relationshipCount;
        protected long projectMillis;

        protected Builder(GraphProjectConfig config) {
            this.graphName = config.graphName();
        }

        public Builder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder withRelationshipCount(long relationshipCount) {
            this.relationshipCount = relationshipCount;
            return this;
        }

        public void withProjectMillis(long projectMillis) {
            this.projectMillis = projectMillis;
        }

        public abstract GraphProjectResult build();
    }
}
