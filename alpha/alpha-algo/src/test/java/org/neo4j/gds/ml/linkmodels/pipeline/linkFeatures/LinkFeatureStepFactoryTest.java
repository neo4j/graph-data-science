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
package org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.CosineFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.ImmutableLinkFeatureStepConfiguration;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.L2FeatureStep;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class LinkFeatureStepFactoryTest {

    @Test
    void testCreateHadamard() {
        List<String> nodeProperties = List.of("noise", "z", "array");
        var step = LinkFeatureStepFactory.create(
            "hadaMard",
            ImmutableLinkFeatureStepConfiguration.builder().nodeProperties(nodeProperties).build()
        );

        assertThat(step).isInstanceOf(HadamardFeatureStep.class);
        assertThat(step.inputNodeProperties()).isEqualTo(nodeProperties);
    }

    @Test
    void testCreateCosine() {
        List<String> nodeProperties = List.of("noise", "z", "array");
        var step = LinkFeatureStepFactory.create(
            "coSine",
            ImmutableLinkFeatureStepConfiguration.builder().nodeProperties(nodeProperties).build()
        );

        assertThat(step).isInstanceOf(CosineFeatureStep.class);
        assertThat(step.inputNodeProperties()).isEqualTo(nodeProperties);
    }

    @Test
    void testCreateL2() {
        List<String> nodeProperties = List.of("noise", "z", "array");
        var step = LinkFeatureStepFactory.create(
            "L2",
            ImmutableLinkFeatureStepConfiguration.builder().nodeProperties(nodeProperties).build()
        );

        assertThat(step).isInstanceOf(L2FeatureStep.class);
        assertThat(step.inputNodeProperties()).isEqualTo(nodeProperties);
    }
}
