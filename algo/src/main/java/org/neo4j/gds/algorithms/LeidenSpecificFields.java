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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public final class LeidenSpecificFields implements CommunityStatisticsSpecificFields {

    public static final LeidenSpecificFields EMPTY =
        new LeidenSpecificFields(0,0d, emptyList(), 0L, 0L, false,emptyMap());

    private final long communityCount;
    private final Map<String, Object> communityDistribution;
    private final double modularity;
    private final List<Double> modularities;
    private final long ranLevels;
    private final boolean didConverge;
    private final long nodeCount;

    public static LeidenSpecificFields from(
        long nodeCount,
        double modularity,
        double[] modularities,
        long componentCount,
        long ranLevels,
        boolean didConverge,
        Map<String, Object> componentDistribution
    ) {

        return new LeidenSpecificFields(
            nodeCount,
            modularity,
            Arrays.stream(modularities).boxed().collect(
                Collectors.toList()),
            ranLevels,
            componentCount,
            didConverge,
            componentDistribution
            );
    }

    private LeidenSpecificFields(
        long nodeCount,
        double modularity,
        List<Double> modularities,
        long ranLevels,
        long communityCount,
        boolean didConverge,
        Map<String, Object> communityDistribution
    ) {
        this.communityCount = communityCount;
        this.communityDistribution = communityDistribution;
        this.modularities=modularities;
        this.ranLevels=ranLevels;
        this.modularity=modularity;
        this.nodeCount=nodeCount;
        this.didConverge=didConverge;
    }

    @Override
    public long communityCount() {
        return communityCount;
    }

    @Override
    public Map<String, Object> communityDistribution() {
        return communityDistribution;
    }

    public long ranLevels() {
        return ranLevels;
    }

    public List<Double> modularities(){return modularities;}

    public double modularity(){ return  modularity;}

    public boolean didConverge(){ return  didConverge;}

    public long nodeCount() { return  nodeCount;}

}
