/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.pagerank;

public class DegreeCache {

    public final static DegreeCache EMPTY = new DegreeCache(new double[0], new double[0][0], 0.0);

    private double[] aggregatedDegrees;
    private double[][] weights;
    private double averageDegree;

    public DegreeCache(double[] aggregatedDegrees, double[][] weights, double averageDegree) {
        this.aggregatedDegrees = aggregatedDegrees;
        this.weights = weights;
        this.averageDegree = averageDegree;
    }

    double[] aggregatedDegrees() {
        return aggregatedDegrees;
    }

    double[][] weights() {
        return weights;
    }

    double average() {
        return averageDegree;
    }
}
