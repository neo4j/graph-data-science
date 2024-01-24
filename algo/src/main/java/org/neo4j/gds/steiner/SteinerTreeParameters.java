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
package org.neo4j.gds.steiner;

import java.util.List;

public final class SteinerTreeParameters {
    static SteinerTreeParameters create(int concurrency, long sourceNode, List<Long> targetNodes, double delta, boolean applyRerouting) {
        return new SteinerTreeParameters(
            concurrency,
            sourceNode,
            targetNodes,
            delta,
            applyRerouting
        );
    }

    private final int concurrency;
    private final long sourceNode;
    private final List<Long> targetNodes;
    private final double delta;
    private final boolean applyRerouting;

    private SteinerTreeParameters(
        int concurrency,
        long sourceNode,
        List<Long> targetNodes,
        double delta,
        boolean applyRerouting
    ) {
        this.concurrency = concurrency;
        this.sourceNode = sourceNode;
        this.targetNodes = targetNodes;
        this.delta = delta;
        this.applyRerouting = applyRerouting;
    }

    int concurrency() {
        return concurrency;
    }

    long sourceNode() {
        return sourceNode;
    }

    List<Long> targetNodes() {
        return targetNodes;
    }

    double delta() {
        return delta;
    }

    boolean applyRerouting() {
        return applyRerouting;
    }
}
