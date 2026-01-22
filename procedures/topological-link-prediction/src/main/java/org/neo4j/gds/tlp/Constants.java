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
package org.neo4j.gds.tlp;

final class Constants {
    static final String ADAMIC_ADAR_INDEX_DESCRIPTION = "Given two nodes, calculate Adamic Adar similarity";
    static final String COMMON_NEIGHBOURS_DESCRIPTION = "Given two nodes, returns the number of common neighbors";
    static final String PREFERENTIAL_ATTACHMENT_DESCRIPTION = "Given two nodes, calculate Preferential Attachment";
    static final String RESOURCE_ALLOCATION_SIMILARITY_DESCRIPTION = "Given two nodes, calculate Resource Allocation similarity";
    static final String SAME_COMMUNITY_DESCRIPTION = "Given two nodes, indicates if they have the same community";
    static final String TOTAL_NEIGHBORS_DESCRIPTION = "Given two nodes, calculate Total Neighbors";

    private Constants() {}
}
