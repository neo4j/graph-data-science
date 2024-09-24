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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class StandardStreamPathCreator {

    private static final String COST_PROPERTY_NAME = "cost";
    private static final String RELATIONSHIP_TYPE_TEMPLATE = "PATH_%d";

    private StandardStreamPathCreator() {}

    public static Path create(
        NodeLookup nodeLookup,
        long[] nodeIds,
        double[] costs,
        long pathIndex
    ) {
        var relationshipType = RelationshipType.withName(formatWithLocale(RELATIONSHIP_TYPE_TEMPLATE, pathIndex));

        return PathFactory.create(
            nodeLookup,
            nodeIds,
            costs,
            relationshipType,
            COST_PROPERTY_NAME
        );
    }

    public static Path create(
        NodeLookup nodeLookup,
        List<Long> nodeIds,
        List<Double> costs,
        long pathIndex
    ) {
        var relationshipType = RelationshipType.withName(formatWithLocale(RELATIONSHIP_TYPE_TEMPLATE, pathIndex));

        return PathFactory.create(
            nodeLookup,
            nodeIds,
            costs,
            relationshipType,
            COST_PROPERTY_NAME
        );
    }
}
