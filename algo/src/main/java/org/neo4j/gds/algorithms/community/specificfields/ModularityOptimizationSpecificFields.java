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
package org.neo4j.gds.algorithms.community.specificfields;

import java.util.Collections;
import java.util.Map;

public class ModularityOptimizationSpecificFields implements CommunityStatisticsSpecificFields {

    public static final ModularityOptimizationSpecificFields EMPTY = new ModularityOptimizationSpecificFields(
        0d,
        0L,
        false,
        0L,
        0L,
        Collections.emptyMap()
    );
    private final double modularity;
    private final long ranIterations;
    private final boolean didConverge;
    private final long nodes;
    private final long communityCount;
    private final Map<String, Object> communityDistribution;

    public ModularityOptimizationSpecificFields(
        double modularity,
        long ranIterations,
        boolean didConverge,
        long nodes,
        long communityCount,
        Map<String, Object> communityDistribution
    ) {
        this.modularity = modularity;
        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
        this.nodes = nodes;
        this.communityCount = communityCount;
        this.communityDistribution = communityDistribution;
    }


    public double modularity() {
        return modularity;
    }

    public long ranIterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    public long nodes() {
        return nodes;
    }

    @Override
    public long communityCount() {
        return communityCount;
    }

    @Override
    public Map<String, Object> communityDistribution() {
        return communityDistribution;
    }
}
