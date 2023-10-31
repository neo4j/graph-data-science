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

public class KnnSpecificFields implements SimilaritySpecificFields {

    public static final KnnSpecificFields EMPTY = new KnnSpecificFields(
        0,
        0,
        false,
        0,
        0,
        Map.of()
    );

    private final long nodesCompared;
    private final long relationshipsWritten;
    private final long ranIterations;
    private final boolean didConverge;
    private final long nodePairsConsidered;
    private final Map<String, Object> similarityDistribution;


    public KnnSpecificFields(
        long nodesCompared,
        long nodePairsConsidered,
        boolean didConverge,
        long ranIterations,
        long relationshipsWritten,
        Map<String, Object> similarityDistribution
    ) {
        this.nodesCompared = nodesCompared;
        this.relationshipsWritten=relationshipsWritten;
        this.similarityDistribution = similarityDistribution;
        this.nodePairsConsidered=nodePairsConsidered;
        this.didConverge=didConverge;
        this.ranIterations=ranIterations;
    }


    @Override
    public long nodesCompared(){
        return nodesCompared;
    }

    @Override
    public long relationshipsWritten(){
        return relationshipsWritten;
    }

    public long nodePairsConsidered(){
        return nodePairsConsidered;
    }

    public long ranIterations(){return ranIterations;}

    public boolean didConverge(){ return  didConverge;}
    @Override
    public Map<String, Object> similarityDistribution(){
        return similarityDistribution;
    }


}
