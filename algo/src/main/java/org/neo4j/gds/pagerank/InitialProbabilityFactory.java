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
package org.neo4j.gds.pagerank;

import org.neo4j.gds.InputNodes;
import org.neo4j.gds.ListInputNodes;
import org.neo4j.gds.MapInputNodes;

import java.util.HashMap;
import java.util.function.LongUnaryOperator;

public final class InitialProbabilityFactory {

    private InitialProbabilityFactory() {}

    public static InitialProbabilityProvider create(
        LongUnaryOperator toMappedId,
        double alpha,
        InputNodes sourceNodes
    ) {
        if (sourceNodes == InputNodes.EMPTY_INPUT_NODES) {
            return new GlobalRestartProbability(alpha);
        } else if (sourceNodes instanceof ListInputNodes) {
            var newSourceNodes = sourceNodes.inputNodes()
                .stream()
                .mapToLong(toMappedId::applyAsLong)
                .boxed()
                .toList();
            return new SourceBasedRestartProbabilityList(alpha, newSourceNodes);
        } else if (sourceNodes instanceof MapInputNodes) {
            var newMap = new HashMap<Long, Double>();
            for (var entry : ((MapInputNodes) sourceNodes).map().entrySet()) {
                var newKey = toMappedId.applyAsLong(entry.getKey());
                newMap.put(newKey, entry.getValue());
            }
            return new SourceBasedRestartProbability(alpha, newMap);
        } else {
            throw new IllegalArgumentException("Unsupported source nodes type");
        }
    }
}
