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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.scaling.Center;
import org.neo4j.gds.scaling.LogScaler;
import org.neo4j.gds.scaling.ScalerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RankDistributionHelpersTest {

    @Test
    void shouldComputeEmptyDistributionOtherWise(){
        var idMap = mock(IdMap.class);
        when(idMap.nodeCount()).thenReturn(3L);

        var distribution = RankDistributionHelpers.compute(
            idMap,
            ScalerFactory.parse(Center.TYPE),
            (i) -> i+1,
            new Concurrency(1),
            true
        );
        var summary = distribution.centralitySummary();
        assertThat(summary.get("min")).asInstanceOf(DOUBLE).isCloseTo(1.0, Offset.offset(1e-4));
        assertThat(summary.get("max")).asInstanceOf(DOUBLE).isEqualTo(3.0,Offset.offset(1e-4));
        assertThat(summary.get("mean")).asInstanceOf(DOUBLE).isEqualTo(2.0,Offset.offset(1e-4));
        assertThat(distribution.computeMillis()).isNotNegative();
    }

    @Test
    void shouldReturnFailForIncorrectScale() {
        var idMap = mock(IdMap.class);

        var distribution = RankDistributionHelpers.compute(
            idMap,
            ScalerFactory.parse(LogScaler.TYPE),
            (i) -> i+1,
            new Concurrency(1),
            true
        );
        var summary = distribution.centralitySummary();
        assertThat(summary).containsKey("Error");
        assertThat(summary).containsValue("Unable to create histogram when using scaler of type LOG");
        assertThat(distribution.computeMillis()).isEqualTo(0);

    }

}
