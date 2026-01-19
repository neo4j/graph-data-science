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
package org.neo4j.gds.procedures.algorithms.community.mutate;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.community.StandardCommunityProperties;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WccMutateResultTransformerTest {

    @Test
    void shouldTransformResult(){

        var config = Map.of("a",(Object)("foo"));
        var result = new HugeAtomicDisjointSetStruct(10,new Concurrency(1));
        result.union(0,1);
        var instructions = mock(StatisticsComputationInstructions.class);
        when(instructions.computeCountAndDistribution()).thenReturn(true);

        var  mutateService =  mock(MutateNodePropertyService.class);
        when(mutateService.mutateNodeProperties(
            any(Graph.class),
            any(GraphStore.class),
            any(MutateNodePropertyService.MutateNodePropertySpec.class),
            any(NodePropertyValues.class))
        ).thenReturn(new NodePropertiesWritten(42));

        var transformer = new WccMutateResultTransformer(
            config,instructions,
            new Concurrency(1),
            mutateService,
            Set.of("bar"),
            "foo",
            mock(Graph.class),
            mock(GraphStore.class),
            new StandardCommunityProperties(true, null,false,"testtest")
            );

        var mutateResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(mutateResult.findFirst().orElseThrow())
            .satisfies(
                mutate ->{
                    assertThat(mutate.componentCount()).isEqualTo(9);
                    assertThat(mutate.computeMillis()).isEqualTo(10);
                    assertThat(mutate.postProcessingMillis()).isNotNegative();
                    assertThat(mutate.componentDistribution()).containsKey("p99");
                    assertThat(mutate.mutateMillis()).isNotNegative();
                    assertThat((mutate.nodePropertiesWritten())).isEqualTo(42L);
                }
            );

    }

}
