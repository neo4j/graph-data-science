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
package org.neo4j.gds.procedures.embeddings;

import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.embeddings.specificfields.Node2VecSpecificFields;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecWriteResult;

public class Node2VecComputationalResultTransformer {

    public static Node2VecWriteResult toWriteResult(NodePropertyWriteResult<Node2VecSpecificFields> writeResult) {

        return new Node2VecWriteResult(
            writeResult.algorithmSpecificFields().nodeCount(),
            writeResult.nodePropertiesWritten(),
            writeResult.preProcessingMillis(),
            writeResult.computeMillis(),
            writeResult.writeMillis(),
            writeResult.configuration().toMap(),
            writeResult.algorithmSpecificFields().lossPerIteration()
        );
    }
}
