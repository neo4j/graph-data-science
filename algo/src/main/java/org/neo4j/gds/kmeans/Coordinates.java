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

import java.util.List;

public final class Coordinates {

    private final Coordinate[] coordinates;
    private final int k;
    private final int dimensions;

    private Coordinates(Coordinate[] coordinates, int k, int dimensions) {
        this.coordinates = coordinates;
        this.k = k;
        this.dimensions = dimensions;
    }

    static Coordinates create(
        int k,
        int dimensions,
        CoordinatesSupplier coordinatesSupplier
    ) {
        var coordinates = new Coordinate[k];
        for (int i = 0; i < k; ++i) {
            coordinates[i] = coordinatesSupplier.get();
        }
        return new Coordinates(coordinates, k, dimensions);
    }


    public void addTo(long nodeId, int coordinateId) {
        coordinates[coordinateId].addTo(nodeId);
    }

    void normalizeDimension(int coordinateId, long length) {
        coordinates[coordinateId].normalize(length);
    }

    public void assign(List<List<Double>> seededCoordinates) {
        int currentlyAssigned = 0;
        for (List<Double> coordinate : seededCoordinates) {
            coordinates[currentlyAssigned++].assign(coordinate);
        }
    }


    public void add(int coordinateId, Coordinates outsideCoordinates) {
        coordinates[coordinateId].add(outsideCoordinates.coordinateAt(coordinateId));
    }

    public void reset(int coordinateId) {
        coordinates[coordinateId].reset();
    }

    Coordinate coordinateAt(int coordinateId) {
        return coordinates[coordinateId];
    }

    double[][] coordinates() {
        double[][] doubleCoordinates = new double[k][dimensions];
        for (int i = 0; i < k; ++i) {
            doubleCoordinates[i] = coordinates[i].coordinate();
        }
        return doubleCoordinates;
    }

    void assignTo(long nodeId, int coordinateId) {
        coordinates[coordinateId].assign(nodeId);
    }
}
