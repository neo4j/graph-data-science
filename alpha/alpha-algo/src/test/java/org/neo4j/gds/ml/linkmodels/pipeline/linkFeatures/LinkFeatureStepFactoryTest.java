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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.CosineFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.L2FeatureStep;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStep.FEATURE_PROPERTIES;

final class LinkFeatureStepFactoryTest {

    @Test
    public void testCreateHadamard() {
        List<String> featureProperties = List.of("noise", "z", "array");
        var step = LinkFeatureStepFactory.create(
            "hadaMard",
            Map.of("featureProperties", featureProperties)
        );

        assertThat(step).isInstanceOf(HadamardFeatureStep.class);
        assertThat(((HadamardFeatureStep) step).featureProperties()).isEqualTo(featureProperties);
    }

    @Test
    public void testCreateCosine() {
        List<String> featureProperties = List.of("noise", "z", "array");
        var step = LinkFeatureStepFactory.create(
            "coSine",
            Map.of("featureProperties", featureProperties)
        );

        assertThat(step).isInstanceOf(CosineFeatureStep.class);
        assertThat(((CosineFeatureStep) step).featureProperties()).isEqualTo(featureProperties);
    }

    @Test
    public void testCreateL2() {
        List<String> featureProperties = List.of("noise", "z", "array");
        var step = LinkFeatureStepFactory.create(
            "L2",
            Map.of("featureProperties", featureProperties)
        );

        assertThat(step).isInstanceOf(L2FeatureStep.class);
        assertThat(((L2FeatureStep) step).featureProperties()).isEqualTo(featureProperties);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory#values")
    public void shouldFailOnMissingFeatureProperties(LinkFeatureStepFactory factory) {
        assertThatThrownBy(() -> LinkFeatureStepFactory.create(factory.name(), Map.of()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("is missing `featureProperties`");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory#values")
    public void shouldFailOnEmptyFeatureProperties(LinkFeatureStepFactory factory) {
        assertThatThrownBy(() -> LinkFeatureStepFactory.create(factory.name(), Map.of(FEATURE_PROPERTIES, List.of())))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires a non-empty list of strings for `featureProperties`");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory#values")
    public void shouldFailOnNotListFeatureProperties(LinkFeatureStepFactory factory) {
        assertThatThrownBy(() -> LinkFeatureStepFactory.create(factory.name(), Map.of(FEATURE_PROPERTIES, Map.of())))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("expects `featureProperties` to be a list of strings");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory#values")
    public void shouldFailOnListOfNonStringsFeatureProperties(LinkFeatureStepFactory factory) {
        assertThatThrownBy(() -> LinkFeatureStepFactory.create(factory.name(), Map.of(FEATURE_PROPERTIES, List.of("foo", 3))))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("expects `featureProperties` to be a list of strings");
    }
}
