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
package org.neo4j.gds.wcc;

import org.neo4j.gds.annotation.Parameters;

@Parameters
public final class WccParameters {

    public static WccParameters create(
        double threshold,
        String seedProperty,
        int concurrency
    ) {
        return new WccParameters(threshold, seedProperty, concurrency);
    }

    private final double threshold;
    private final String seedProperty;
    private final int concurrency;

    private WccParameters(double threshold, String seedProperty, int concurrency) {
        this.threshold = threshold;
        this.seedProperty = seedProperty;
        this.concurrency = concurrency;
    }

    double threshold() {
        return threshold;
    }

    boolean hasThreshold() {
        return !Double.isNaN(threshold()) && threshold() > 0;
    }

    String seedProperty() {
        return seedProperty;
    }

    public boolean isIncremental() {
        return seedProperty != null;
    }

    int concurrency() {
        return concurrency;
    }
}
