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
package org.neo4j.gds.algorithms.centrality.specificfields;

import java.util.Map;

public class PageRankSpecificFields implements CentralityStatisticsSpecificFields {

    public static final PageRankSpecificFields EMPTY = new PageRankSpecificFields(
        0,
        false,
        Map.of()
    );

    private final long ranIterations;
    private final boolean didConverge;
    private final Map<String, Object> centralityDistribution;

    public PageRankSpecificFields(
        long ranIterations,
        boolean didConverge,
        Map<String, Object> centralityDistribution
    ) {
        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
        this.centralityDistribution = centralityDistribution;
    }

    public long ranIterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    @Override
    public Map<String, Object> centralityDistribution() {
        return centralityDistribution;
    }
}
