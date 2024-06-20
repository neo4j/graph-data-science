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
package org.neo4j.gds.procedures.community;

import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.community.specificfields.LocalClusteringCoefficientSpecificFields;
import org.neo4j.gds.procedures.community.triangle.LocalClusteringCoefficientWriteResult;

final class LCCComputationResultTransformer {

    private LCCComputationResultTransformer() {}

    static LocalClusteringCoefficientWriteResult toWriteResult(
        NodePropertyWriteResult<LocalClusteringCoefficientSpecificFields> computationResult
    ) {
        return new LocalClusteringCoefficientWriteResult(
            computationResult.algorithmSpecificFields().averageClusteringCoefficient(),
            computationResult.algorithmSpecificFields().nodeCount(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.writeMillis(),
            computationResult.nodePropertiesWritten(),
            computationResult.configuration().toMap()
        );
    }


}
