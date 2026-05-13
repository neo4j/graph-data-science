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
package org.neo4j.gds.scaling.scale;

import java.util.List;
import java.util.Map;

public final class Zero extends ScalarScaler {
    private final Map<String, List<Double>> statistics;

    private Zero(Map<String, List<Double>> statistics) {
        this.statistics = statistics;
    }

    public static Zero of(Map<String, List<Double>> stats) {
        return new Zero(stats);
    }

    public static Zero of() {
        return of(Map.of());
    }

    @Override
    public Map<String, List<Double>> statistics() {
        return statistics;
    }

    @Override
    public double scaleProperty(long nodeId) {
        return 0;
    }
}
