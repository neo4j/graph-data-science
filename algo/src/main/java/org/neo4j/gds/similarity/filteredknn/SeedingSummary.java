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
package org.neo4j.gds.similarity.filteredknn;

public class SeedingSummary {

    private final long nodesCompared;
    private final long nodePairsCompared;
    private final boolean seededOptimally;

    static SeedingSummary EMPTY_SEEDING_SUMMARY = new SeedingSummary(0,0,false);

    SeedingSummary(long nodesCompared, long nodePairsCompared,boolean seededOptimally) {
        this.nodesCompared = nodesCompared;
        this.nodePairsCompared = nodePairsCompared;
        this.seededOptimally = seededOptimally;
    }

    public long nodesCompared(){
        return nodesCompared;
    }
    public long nodePairsCompared(){
        return nodePairsCompared;
    }

    public boolean seededOptimally(){
        return seededOptimally;
    }
}
