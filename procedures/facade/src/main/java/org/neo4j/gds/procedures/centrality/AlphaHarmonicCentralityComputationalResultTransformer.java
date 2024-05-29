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
package org.neo4j.gds.procedures.centrality;

import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.centrality.specificfields.AlphaHarmonicSpecificFields;
import org.neo4j.gds.harmonic.DeprecatedTieredHarmonicCentralityWriteConfig;
import org.neo4j.gds.procedures.centrality.alphaharmonic.AlphaHarmonicWriteResult;

final class AlphaHarmonicCentralityComputationalResultTransformer {

    private AlphaHarmonicCentralityComputationalResultTransformer() {}

    static AlphaHarmonicWriteResult toWriteResult(
        NodePropertyWriteResult<AlphaHarmonicSpecificFields> computationResult,
        DeprecatedTieredHarmonicCentralityWriteConfig config
    ) {
        return new AlphaHarmonicWriteResult(
            computationResult.algorithmSpecificFields().nodes(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.writeMillis(),
            config.writeProperty(),
            computationResult.algorithmSpecificFields().centralityDistribution()
        );
    }

}
