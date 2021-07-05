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
package org.neo4j.gds.ml.pipeline.linkfunctions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkFeatureStepFactory;
import org.neo4j.gds.ml.linkmodels.pipeline.linkfunctions.CosineFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkfunctions.L2FeatureStep;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class LinkFeatureStepFactoryTest {

    @Test
    public void testCreateHadamard() {
        List<String> featureProperties = List.of("noise", "z", "array");
        var step = LinkFeatureStepFactory.create(
            "hadaMard",
            Map.of("featureProperties", featureProperties)
        );

        assertThat(step instanceof HadamardFeatureStep);
        var actual = (HadamardFeatureStep) step;

        assertEquals(featureProperties, actual.featureProperties());

    }

    @Test
    public void testCreateCosine() {
        List<String> featureProperties = List.of("noise", "z", "array");
        var step = LinkFeatureStepFactory.create(
            "coSine",
            Map.of("featureProperties", featureProperties)
        );

        assertThat(step instanceof CosineFeatureStep);
        var actual = (CosineFeatureStep) step;

        assertEquals(featureProperties, actual.featureProperties());

    }

    @Test
    public void testCreateL2() {
        List<String> featureProperties = List.of("noise", "z", "array");
        var step = LinkFeatureStepFactory.create(
            "L2",
            Map.of("featureProperties", featureProperties)
        );

        assertThat(step instanceof L2FeatureStep);
        var actual = (L2FeatureStep) step;

        assertEquals(featureProperties, actual.featureProperties());

    }
}
