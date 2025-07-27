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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.Intersections;

import java.util.Arrays;
import java.util.List;

public class DoubleArrayCoordinates implements  Coordinates{

    private final double[][] coordinates;
    private final int dimensions;
    private final NodePropertyValues nodePropertyValues;

    DoubleArrayCoordinates(int k,int dimensions, NodePropertyValues nodePropertyValues){
        this.nodePropertyValues = nodePropertyValues;
        this.coordinates =new double[k][dimensions];
        this.dimensions = dimensions;
    }
    @Override
    public void addTo(long nodeId, int coordinateId) {
        var property = nodePropertyValues.doubleArrayValue(nodeId);
        for (int j = 0; j < dimensions; ++j) {
            coordinates[coordinateId][j] += property[j];
        }
    }

    @Override
    public void reset(int coordinateId) {
        Arrays.fill(coordinates[coordinateId],0.0d);
    }

    @Override
    public double euclideanDistance(long nodeId, int coordinateId) {
        double[] left = nodePropertyValues.doubleArrayValue(nodeId);
        double[] right = coordinates[coordinateId];
        return Math.sqrt(Intersections.sumSquareDelta(left, right, right.length));
    }

    @Override
    public void normalizeDimension(int coordinateId, int dimension, long length) {
        coordinates[coordinateId][dimension]/=(double) length;
    }
    @Override
    public double[][] coordinates() {
        return coordinates;
    }

    @Override
    public void assign( List<List<Double>>  seededCoordinates) {
        int currentlyAssigned =0;
        for (List<Double> coordinate : seededCoordinates) {
            var coordinateArray = new double[dimensions];
            int index = 0;
            for (double value : coordinate) {
                coordinateArray[index++] =  value;
            }
            System.arraycopy(coordinateArray, 0, coordinates[currentlyAssigned++], 0, coordinateArray.length);
        }
    }

    @Override
    public void assignTo(long nodeId, int coordinateId) {
        double[] cluster = nodePropertyValues.doubleArrayValue(nodeId);
        System.arraycopy(cluster, 0, coordinates[coordinateId], 0, cluster.length);
    }

    private double coordinateAt(int coordinateId, int dimension){
        return  coordinates[coordinateId][dimension];
    }

    @Override
    public void add(int coordinateId, Coordinates outsideCoordinates) {
        if (outsideCoordinates instanceof  DoubleArrayCoordinates doubleArrayCoordinates) {
            for (int dimension = 0; dimension < dimensions; ++dimension) {
                coordinates[coordinateId][dimension] += doubleArrayCoordinates.coordinateAt(coordinateId,dimension);
            }
        }else {
            throw new RuntimeException("Only works for double coordinates");
        }
    }

}
