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
package org.neo4j.gds.modularityoptimization;

import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;

import java.util.function.LongUnaryOperator;

public class ModularityOptimizationResult {

    private final LongUnaryOperator communityIdLookup;
    private final double modularity;
    private final int ranIterations;
    private final boolean didConverge;
    private final long nodeCount;

    public ModularityOptimizationResult(
        LongUnaryOperator communityIdLookup,
        double modularity,
        int ranIterations,
        boolean didConverge,
        long nodeCount
    ) {
        this.communityIdLookup = communityIdLookup;
        this.nodeCount = nodeCount;

        this.modularity = modularity;
        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
    }

    public double modularity() {
        return modularity;
    }

    public int ranIterations() {
        return ranIterations;
    }

    public long communityId(long nodeId) {
        return communityIdLookup.applyAsLong(nodeId);
    }

    public boolean didConverge() {
        return didConverge;
    }

    public LongNodePropertyValues asNodeProperties() {
        return new LongNodePropertyValues() {
            @Override
            public long longValue(long nodeId) {
                return communityId(nodeId);
            }

            @Override
            public long size() {
                return nodeCount;
            }
        };
    }
}
