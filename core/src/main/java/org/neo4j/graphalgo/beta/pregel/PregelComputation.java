/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel;

import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Map;
import java.util.Queue;

import static org.neo4j.graphalgo.beta.pregel.Pregel.DEFAULT_NODE_VALUE_KEY;

@FunctionalInterface
public interface PregelComputation<C extends PregelConfig> {

    default Map<String, ValueType> nodeValueSchema() {
        return Map.of(DEFAULT_NODE_VALUE_KEY, ValueType.DOUBLE);
    }

    default void init(PregelContext<C> context, long nodeId) {}

    void compute(PregelContext<C> context, long nodeId, Queue<Double> messages);

    default double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
        return nodeValue;
    }
}
