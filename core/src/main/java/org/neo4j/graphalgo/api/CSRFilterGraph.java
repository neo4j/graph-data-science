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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.RelationshipType;

import java.util.Map;
import java.util.Set;

public abstract class CSRFilterGraph extends MultiFilterGraph implements CSRGraph {

    protected final CSRGraph graph;

    public CSRFilterGraph(CSRGraph graph) {
        super(graph);
        this.graph = graph;
    }

    @Override
    public CSRGraph concurrentCopy() {
        return graph.concurrentCopy();
    }

    @Override
    public Map<RelationshipType, Relationships.Topology> relationshipTopologies() {
        return graph.relationshipTopologies();
    }

    @Override
    public Set<RelationshipType> relationshipTypes(long source, long target) {
        return graph.relationshipTypes();
    }

    @Override
    public Set<RelationshipType> availableRelationshipTypes() {
        return graph.availableRelationshipTypes();
    }
}
