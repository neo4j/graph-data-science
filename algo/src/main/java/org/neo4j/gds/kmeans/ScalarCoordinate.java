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

import java.util.List;

class ScalarCoordinate implements Coordinate {

    private double value;
    private final NodePropertyValues nodePropertyValues;

    ScalarCoordinate(NodePropertyValues nodePropertyValues) {
        this.value = 0;
        this.nodePropertyValues = nodePropertyValues;
    }

    @Override
    public double[] coordinate() {
        return new double[]{value};
    }

    @Override
    public void reset() {
        value = 0;
    }

    @Override
    public void assign(long nodeId) {
        value = nodePropertyValues.doubleValue(nodeId);

    }

    @Override
    public void assign(List<Double> coordinate) {
        value = coordinate.getFirst();
    }

    @Override
    public void normalize(long length) {
        value /= (double) length;
    }

    @Override
    public void add(Coordinate externalCoordinate) {
        var scalarCoordinate = (ScalarCoordinate) externalCoordinate;
        value += scalarCoordinate.value();
    }

    @Override
    public void addTo(long nodeId) {
        var property = nodePropertyValues.doubleValue(nodeId);
        value += property;
    }

    double value() {
        return value;
    }

}
