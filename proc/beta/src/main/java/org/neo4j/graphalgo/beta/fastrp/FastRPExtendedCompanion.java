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
package org.neo4j.graphalgo.beta.fastrp;

import org.neo4j.gds.beta.fastrp.FastRPExtendedBaseConfig;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.FloatArrayNodeProperties;

final class FastRPExtendedCompanion {

    static final String DESCRIPTION = "The FastRPExtended algorithm produces node embeddings via random projection of nodes and their properties";

    private FastRPExtendedCompanion() {}

    static <CONFIG extends FastRPExtendedBaseConfig> NodeProperties getNodeProperties(AlgoBaseProc.ComputationResult<FastRP, FastRP.FastRPResult, CONFIG> computationResult) {
        var size = computationResult.graph().nodeCount();
        var embeddings = computationResult.result().embeddings();

        return new FloatArrayNodeProperties() {
            @Override
            public long size() {
                return size;
            }

            @Override
            public float[] floatArrayValue(long nodeId) {
                return embeddings.get(nodeId);
            }
        };
    }
}
