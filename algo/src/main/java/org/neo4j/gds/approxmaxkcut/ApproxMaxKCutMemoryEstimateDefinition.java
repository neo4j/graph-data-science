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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeByteArray;
import org.neo4j.gds.collections.haa.HugeAtomicByteArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;

public class ApproxMaxKCutMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition {

    private final ApproxMaxKCutMemoryEstimationParameters parameters;

    public ApproxMaxKCutMemoryEstimateDefinition(ApproxMaxKCutMemoryEstimationParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        var builder = MemoryEstimations.builder(ApproxMaxKCut.class);

        builder.perNode("best solution candidate", HugeByteArray::memoryEstimation);
        builder.perNode("solution workspace", HugeByteArray::memoryEstimation);
        builder.perNodeVector(
            "local search improvement costs cache",
            parameters.k(),
            HugeAtomicDoubleArray::memoryEstimation
        );
        builder.perNode("local search set swap status cache", HugeAtomicByteArray::memoryEstimation);

        if (parameters.vnsMaxNeighborhoodOrder() > 0) {
            builder.perNode("vns neighbor solution candidate", HugeByteArray::memoryEstimation);
        }

        return builder.build();
    }

}
