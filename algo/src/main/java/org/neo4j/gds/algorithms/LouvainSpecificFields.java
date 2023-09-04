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

public final class LouvainSpecificFields implements CommunityStatisticsSpecificFields {

    public static final LouvainSpecificFields EMPTY =
        new LouvainSpecificFields(0d, emptyList(), 0L, 0L, emptyMap());

    private final long componentCount;
    private final Map<String, Object> componentDistribution;
    private final double modularity;
    private final List<Double> modularities;
    private final long ranLevels;

    public static LouvainSpecificFields from(
        double modularity,
        double[] modularities,
        long componentCount,
        long ranLevel,
        Map<String, Object> componentDistribution
    ) {

        return new LouvainSpecificFields(
            modularity,
            Arrays.stream(modularities).boxed().collect(
                Collectors.toList()),
            ranLevel,
            componentCount,
            componentDistribution
        );
    }

    private LouvainSpecificFields(
        double modularity,
        List<Double> modularities,
        long ranLevels,
        long componentCount,
        Map<String, Object> componentDistribution
    ) {
        this.componentCount = componentCount;
        this.componentDistribution = componentDistribution;
        this.modularities=modularities;
        this.ranLevels=ranLevels;
        this.modularity=modularity;
    }

    @Override
    public long componentCount() {
        return componentCount;
    }

    @Override
    public Map<String, Object> componentDistribution() {
        return componentDistribution;
    }

    public long ranLevels() {
        return ranLevels;
    }

    public List<Double> modularities(){return modularities;}

    public double modularity(){ return  modularity;}
}
