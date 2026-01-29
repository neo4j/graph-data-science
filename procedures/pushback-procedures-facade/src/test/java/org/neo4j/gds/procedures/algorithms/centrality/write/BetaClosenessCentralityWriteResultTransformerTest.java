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
package org.neo4j.gds.procedures.algorithms.centrality.write;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.centrality.GenericCentralityWriteStep;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BetaClosenessCentralityWriteResultTransformerTest {

    @Test
    void shouldTransform() {
        var result = mock(ClosenessCentralityResult.class);
        when(result.nodePropertyValues()).thenReturn(mock(NodePropertyValues.class));

        var config = Map.of("a",(Object)("foo"));

        var writeStep = mock(GenericCentralityWriteStep.class);
        when(writeStep.execute(any(Graph.class),any(GraphStore.class),any(ResultStore.class),eq(result),any(JobId.class)))
            .thenReturn(new NodePropertiesWritten(42));

        var transformer = new  BetaClosenessCentralityWriteResultTransformer(
            mock(Graph.class),
            mock(GraphStore.class),
            config,
            true,
            new Concurrency(1),
            writeStep,
            mock(JobId.class),
            mock(ResultStore.class),
            "foo"
        );

        var writeResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));
        assertThat(writeResult.findFirst().orElseThrow())
            .satisfies(write -> {
                assertThat(write.preProcessingMillis()).isEqualTo(0);
                assertThat(write.computeMillis()).isEqualTo(10);
                assertThat(write.postProcessingMillis()).isGreaterThanOrEqualTo(0);
                assertThat(write.configuration()).isEqualTo(config);
                assertThat(write.writeMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(write.centralityDistribution()).containsKey("p99");
                assertThat(write.nodePropertiesWritten()).isEqualTo(42L);
                assertThat(write.writeProperty()).isEqualTo("foo");

            });
    }

}
