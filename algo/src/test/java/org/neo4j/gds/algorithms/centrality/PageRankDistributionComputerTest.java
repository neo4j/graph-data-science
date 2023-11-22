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
package org.neo4j.gds.algorithms.centrality;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.scaling.ScalerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PageRankDistributionComputerTest {

    @Test
    void shouldReturnDistributionWithErrorHint() {
        var pageRankConfigMock = mock(PageRankConfig.class);
        when(pageRankConfigMock.scaler()).thenReturn(ScalerFactory.parse("log"));
        var pageRankDistribution = PageRankDistributionComputer.computeDistribution(
            null,
            pageRankConfigMock,
            true
        );

        assertThat(pageRankDistribution).isNotNull();
        assertThat(pageRankDistribution.postProcessingMillis).isEqualTo(0);
        assertThat(pageRankDistribution.centralitySummary)
            .hasSize(1)
            .containsEntry("Error", "Unable to create histogram when using scaler of type LOG");
    }

    @Test
    void shouldReturnComputedDistribution() {
        var pageRankConfigMock = mock(PageRankConfig.class);
        when(pageRankConfigMock.scaler()).thenReturn(ScalerFactory.parse("none"));

        var pageRankResultMock = mock(PageRankResult.class);
        when(pageRankResultMock.nodeCount()).thenReturn(190L);
        when(pageRankResultMock.centralityScoreProvider()).thenReturn(n -> (double) n);
        var pageRankDistribution = PageRankDistributionComputer.computeDistribution(
            pageRankResultMock,
            pageRankConfigMock,
            true
        );

        assertThat(pageRankDistribution).isNotNull();
        assertThat(pageRankDistribution.postProcessingMillis).isPositive();
        assertThat(pageRankDistribution.centralitySummary)
            .hasSize(9)
            .containsKeys("max", "mean", "min", "p50", "p75", "p90", "p95", "p99", "p999")
            .doesNotContainKey("Error");

    }
}
