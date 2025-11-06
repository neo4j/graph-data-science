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
package org.neo4j.gds.community;

import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;

 class StandardCommunityProperties {

    private final boolean isIncremental;
    private final String seedProperty;
    private final boolean consecutiveIds;
    private final String resultProperty;

    StandardCommunityProperties(
        boolean isIncremental,
        String seedProperty,
        boolean consecutiveIds,
        String resultProperty
    ) {
        this.isIncremental = isIncremental;
        this.seedProperty = seedProperty;
        this.consecutiveIds = consecutiveIds;
        this.resultProperty = resultProperty;
    }

    NodePropertyValues compute(
        GraphStore graphStore,
        LongNodePropertyValues algorithmNodePropertyValues
    ) {

        return CommunityCompanion.nodePropertyValues(
            isIncremental,
            resultProperty,
            seedProperty,
            consecutiveIds,
            algorithmNodePropertyValues,
            () -> graphStore.nodeProperty(seedProperty)
        );
    }
}
