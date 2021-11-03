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
package org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStepFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@GdlExtension
final class HadamardLinkFeatureStepTest extends FeatureStepBaseTest {

    @Test
    public void runHadamardLinkFeatureStep() {
        var step = LinkFeatureStepFactory.create(
            "hadamard",
            ImmutableLinkFeatureStepConfiguration.builder().nodeProperties(List.of("noise", "z", "array")).build()
        );
        var linkFeatures = LinkFeatureExtractor.extractFeatures(graph, List.of(step), 4, ProgressTracker.NULL_TRACKER);

        var delta = 0.0001D;

        assertArrayEquals(new double[]{42 * 1337, 13 * 0D, 3 * 1D, 2 * 1D}, linkFeatures.get(0), delta);
        assertArrayEquals(new double[]{42 * 42, 13 * 2, 3 * 8, 2 * 2.3D}, linkFeatures.get(1), delta);
        assertArrayEquals(new double[]{42 * 42, 13 * 9, 3 * 0.1D, 2 * 91.0D}, linkFeatures.get(2), delta);
    }

    @Test
    public void handlesZeroVectors() {
        var step = LinkFeatureStepFactory.create(
            "hadamard",
            ImmutableLinkFeatureStepConfiguration.builder().nodeProperties(List.of("zeros")).build()
        );

        var linkFeatures = LinkFeatureExtractor.extractFeatures(graph, List.of(step), 4, ProgressTracker.NULL_TRACKER);

        for (long i = 0; i < linkFeatures.size(); i++) {
            assertThat(linkFeatures.get(i)).containsOnly(0.0);
        }
    }

    @Test
    public void failsOnNaNValues() {
        var step = LinkFeatureStepFactory.create(
            "hadamard",
            ImmutableLinkFeatureStepConfiguration.builder().nodeProperties(List.of("invalidValue", "z")).build()
        );

        assertThatThrownBy(() -> LinkFeatureExtractor.extractFeatures(graph, List.of(step), 4, ProgressTracker.NULL_TRACKER))
            .hasMessage("Encountered NaN in the nodeProperty `invalidValue` for nodes ['1'] when computing the hadamard feature vector. " +
                        "Either define a default value if its a stored property or check the nodePropertyStep");
    }
}
