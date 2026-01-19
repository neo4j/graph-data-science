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
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.beta.pregel.ImmutablePregelResult;
import org.neo4j.gds.beta.pregel.NodeValue;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SpeakerListenerLPAMutateResultTransformerTest {

    @Test
    void shouldTransformResult(){

        var config = Map.of("a",(Object)("foo"));

        var schema= NodeValue.of(new PregelSchema.Builder().add("communityIds", ValueType.LONG_ARRAY).build(),2,new Concurrency(1));
        long[] value1 = {1, 2};
        schema.set("communityIds",0, value1);
        long[] value2 = {2, 1};
        schema.set("communityIds",1, value2);

        var result= ImmutablePregelResult.of(schema, 100, true);

        var mutateService =  mock(MutateNodePropertyService.class);

        when(mutateService.mutateNodeProperties(
            any(Graph.class),
            any(GraphStore.class),
            any(MutateNodePropertyService.MutateNodePropertySpec.class),
            any(NodePropertyValues.class))
        ).thenReturn(new NodePropertiesWritten(42));

        var transformer = new SpeakerListenerLPAMutateResultTransformer(
            config,
            mutateService,
            Set.of("bar"),
            "foo",
            mock(Graph.class),
            mock(GraphStore.class)
        );

        var mutateResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(mutateResult.findFirst().orElseThrow())
            .satisfies(mutate -> {
                assertThat(mutate.ranIterations()).isEqualTo(100);
                assertThat(mutate.didConverge()).isTrue();
                assertThat(mutate.preProcessingMillis()).isEqualTo(0);
                assertThat(mutate.computeMillis()).isEqualTo(10);
                assertThat(mutate.configuration()).isEqualTo(config);
                assertThat(mutate.mutateMillis()).isNotNegative();
                assertThat((mutate.nodePropertiesWritten())).isEqualTo(42L);
            });
    }

}
