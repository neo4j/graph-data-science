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
package org.neo4j.gds.catalog;

final class GraphCatalogProcedureConstants {
    static final String DROP_DESCRIPTION = "Drops a named graph from the catalog and frees up the resources it occupies.";
    static final String DROP_GRAPH_PROPERTY_DESCRIPTION = "Removes a graph property from a projected graph.";
    static final String STREAM_GRAPH_PROPERTY_DESCRIPTION = "Streams the given graph property.";
    static final String DROP_NODE_PROPERTIES_DESCRIPTION = "Removes node properties from a projected graph.";
    static final String DROP_RELATIONSHIPS_DESCRIPTION = "Delete the relationship type for a given graph stored in the graph-catalog.";
    static final String EXISTS_DESCRIPTION = "Checks if a graph exists in the catalog.";
    static final String LIST_DESCRIPTION = "Lists information about named graphs stored in the catalog.";
    static final String PROJECT_DESCRIPTION = "Creates a named graph in the catalog for use by algorithms.";
    static final String STREAM_NODE_PROPERTIES_DESCRIPTION = "Streams the given node properties.";
    static final String STREAM_NODE_PROPERTY_DESCRIPTION = "Streams the given node property.";
    static final String STREAM_RELATIONSHIP_PROPERTIES_DESCRIPTION = "Streams the given relationship properties.";
    static final String STREAM_RELATIONSHIP_PROPERTY_DESCRIPTION = "Streams the given relationship property.";
    static final String STREAM_RELATIONSHIPS_DESCRIPTION = "Streams the given relationship source/target pairs";
    static final String WRITE_NODE_PROPERTIES_DESCRIPTION = "Writes the given node properties to an online Neo4j database.";

    private GraphCatalogProcedureConstants() {}
}
