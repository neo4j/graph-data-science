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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;

import java.util.function.LongPredicate;

public interface LPNodeFilter extends LongPredicate {

    long validNodeCount();

    static LPNodeFilter of(Graph predictGraph, IdMap idMap) {
        // IdMap will only contain nodes that are in the predictGraph.
        if (predictGraph.nodeCount() == idMap.nodeCount()) {
            return new LPNodeFilter() {
                @Override
                public long validNodeCount() {
                    return idMap.nodeCount();
                }

                @Override
                public boolean test(long value) {
                    return true;
                }
            };
        } else {
            return new LPNodeFilter() {
                @Override
                public long validNodeCount() {
                    return idMap.nodeCount();
                }

                @Override
                public boolean test(long id) {
                    return idMap.contains(predictGraph.toOriginalNodeId(id));
                }
            };
        }
    }
}
