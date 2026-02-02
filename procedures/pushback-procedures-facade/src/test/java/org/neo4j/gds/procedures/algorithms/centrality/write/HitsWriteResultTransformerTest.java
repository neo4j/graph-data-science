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
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.beta.pregel.NodeValue;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.centrality.HitsWriteStep;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.hits.HitsResultWithGraph;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HitsWriteResultTransformerTest {

    @Test
    void shouldTransform() {
        var config = Map.of("a", (Object) ("foo"));
        var auth = HugeDoubleArray.of(10, 9, 8);
        var hub = HugeDoubleArray.of(7, 6, 5);
        var nodeValues = mock(NodeValue.class);
        when(nodeValues.doubleProperties(eq("auth"))).thenReturn(auth);
        when(nodeValues.doubleProperties(eq("hub"))).thenReturn(hub);
        var pregelResult = mock(PregelResult.class);
        when(pregelResult.ranIterations()).thenReturn(1234);
        when(pregelResult.didConverge()).thenReturn(true);
        when(pregelResult.nodeValues()).thenReturn(nodeValues);
        var result = mock(HitsResultWithGraph.class);
        when(result.pregelResult()).thenReturn(pregelResult);
        when(result.graph()).thenReturn(mock(Graph.class));
        var writeStep = mock(HitsWriteStep.class);

        when(writeStep.execute(any(Graph.class),any(GraphStore.class),any(ResultStore.class),eq(pregelResult),any(JobId.class)))
            .thenReturn(new NodePropertiesWritten(42));

        var transformer = new HitsWriteResultTransformer(
            mock(GraphStore.class),
            config,
            writeStep,
            mock(JobId.class),
            mock(ResultStore.class)
        );

        var writeResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(writeResult.findFirst().orElseThrow())
            .satisfies(write -> {
                assertThat(write.computeMillis()).isEqualTo(10);
                assertThat(write.configuration()).isEqualTo(config);
                assertThat(write.writeMillis()).isGreaterThanOrEqualTo(0L);
                assertThat(write.didConverge()).isTrue();
                assertThat(write.nodePropertiesWritten()).isEqualTo(42L);
                assertThat(write.ranIterations()).isEqualTo(1234);
            });
    }

}
