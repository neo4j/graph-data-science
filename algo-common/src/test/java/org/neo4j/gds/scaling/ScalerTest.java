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
package org.neo4j.gds.scaling;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.nodeproperties.DoubleTestPropertyValues;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ScalerTest {

    @Test
    void shouldAccumulateStatsCorrectly() {
        var meanScaler1 = (Mean) Mean.buildFrom(CypherMapWrapper.empty()).create(
            new DoubleTestPropertyValues(nodeId -> nodeId),
            10,
            1,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );
        var meanScaler2 = (Mean) Mean.buildFrom(CypherMapWrapper.empty()).create(
            new DoubleTestPropertyValues(nodeId -> 2 * nodeId),
            10,
            1,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        var meanScaler1Stats = meanScaler1.statistics();
        var meanScaler2Stats = meanScaler2.statistics();

        var arrayScalerStats = new Scaler.ArrayScaler(List.of(meanScaler1, meanScaler2), ProgressTracker.NULL_TRACKER).statistics();
        assertThat(arrayScalerStats).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "min", List.of(meanScaler1Stats.get("min").get(0), meanScaler2Stats.get("min").get(0)),
                "max", List.of(meanScaler1Stats.get("max").get(0), meanScaler2Stats.get("max").get(0)),
                "avg", List.of(meanScaler1Stats.get("avg").get(0), meanScaler2Stats.get("avg").get(0))
            )
        );
    }
}
