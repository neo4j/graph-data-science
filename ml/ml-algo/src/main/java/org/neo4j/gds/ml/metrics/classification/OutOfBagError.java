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
package org.neo4j.gds.ml.metrics.classification;

import org.neo4j.gds.ml.metrics.Metric;

import java.util.Comparator;

public final class OutOfBagError implements Metric {
    private OutOfBagError() {
    }

    public static final OutOfBagError OUT_OF_BAG_ERROR = new OutOfBagError();

    @Override
    public String name() {
        return "OUT_OF_BAG_ERROR";
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public Comparator<Double> comparator() {
        return Comparator.naturalOrder();
    }
}
