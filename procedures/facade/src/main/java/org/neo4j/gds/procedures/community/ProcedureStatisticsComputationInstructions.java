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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.result.StatisticsComputationInstructions;

public final class ProcedureStatisticsComputationInstructions implements StatisticsComputationInstructions {
    private final boolean computeCount;
    private final boolean computeDistribution;

    static StatisticsComputationInstructions forComponents(ProcedureReturnColumns procedureReturnColumns) {
        return new ProcedureStatisticsComputationInstructions(
            procedureReturnColumns.contains("componentCount"),
            procedureReturnColumns.contains("componentDistribution")
        );
    }

    static StatisticsComputationInstructions forCommunities(ProcedureReturnColumns procedureReturnColumns) {
        return new ProcedureStatisticsComputationInstructions(
            procedureReturnColumns.contains("communityCount"),
            procedureReturnColumns.contains("communityDistribution")
        );
    }

    private ProcedureStatisticsComputationInstructions(
        boolean computeCount,
        boolean computeDistribution
    ) {
        this.computeCount = computeCount;
        this.computeDistribution = computeDistribution;
    }

    @Override
    public boolean computeCountOnly() {
        return computeCount;
    }

    @Override
    public boolean computeCountAndDistribution() {
        return computeDistribution;
    }
}
