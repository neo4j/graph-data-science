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

import org.neo4j.gds.config.ListSourceNodes;
import org.neo4j.gds.config.MapSourceNodes;
import org.neo4j.gds.config.SourceNodes;

import java.util.HashMap;
import java.util.function.LongUnaryOperator;

public final class InitialProbabilityFactory {

    private InitialProbabilityFactory() {}

    public static InitialProbabilityProvider create(
        LongUnaryOperator toMappedId,
        double alpha,
        SourceNodes sourceNodes
    ) {
        if (sourceNodes == SourceNodes.EMPTY_SOURCE_NODES) {
            return new GlobalRestartProbability(alpha);
        } else if (sourceNodes instanceof ListSourceNodes) {
            var newSourceNodes = sourceNodes.sourceNodes()
                .stream()
                .mapToLong(toMappedId::applyAsLong)
                .boxed()
                .toList();
            return new SourceBasedRestartProbabilityList(alpha, newSourceNodes);
        } else if (sourceNodes instanceof MapSourceNodes) {
            var newMap = new HashMap<Long, Double>();
            for (var entry : ((MapSourceNodes) sourceNodes).map().entrySet()) {
                var newKey = toMappedId.applyAsLong(entry.getKey());
                newMap.put(newKey, entry.getValue());
            }
            return new SourceBasedRestartProbability(alpha, newMap);
        } else {
            throw new IllegalArgumentException("Unsupported source nodes type");
        }
    }
}
