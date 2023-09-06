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
package org.neo4j.gds.algorithms;

import java.util.Map;

public class StandardCommunityStatisticsSpecificFields implements CommunityStatisticsSpecificFields {
    private final long componentCount;
    private final Map<String, Object> componentDistribution;
    public static StandardCommunityStatisticsSpecificFields EMPTY = new StandardCommunityStatisticsSpecificFields(
        0,
        Map.of()
    );

    public StandardCommunityStatisticsSpecificFields(
        long componentCount,
        Map<String, Object> componentDistribution
    ) {
        this.componentCount = componentCount;
        this.componentDistribution = componentDistribution;
    }

    public long componentCount() {
        return componentCount;
    }

    public Map<String, Object> componentDistribution() {
        return componentDistribution;
    }
}
