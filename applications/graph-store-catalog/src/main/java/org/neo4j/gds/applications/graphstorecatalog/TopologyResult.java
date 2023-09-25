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
package org.neo4j.gds.applications.graphstorecatalog;

import java.util.Objects;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class TopologyResult {
    public final long sourceNodeId;
    public final long targetNodeId;
    public final String relationshipType;

    public TopologyResult(long sourceNodeId, long targetNodeId, String relationshipType) {
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
        this.relationshipType = relationshipType;
    }

    @Override
    public String toString() {
        return formatWithLocale("TopologyResult(%d, %d, type: %s)", sourceNodeId, targetNodeId, relationshipType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopologyResult that = (TopologyResult) o;
        return sourceNodeId == that.sourceNodeId && targetNodeId == that.targetNodeId && relationshipType.equals(
            that.relationshipType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNodeId, targetNodeId, relationshipType);
    }
}
