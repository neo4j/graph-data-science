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
package org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStepFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SameCategoryStepTest {

    @Test
    void runSteps() {
        var graph = GdlFactory.of(
            "(a1 {l: 2, d: 42.42})-->(a2 {l: 2L, d: 1337.42})" +
            "(a3 {l: 10, d: 42.42})-->(a4 {l: 2L, d: 42.42})"
        ).build().getUnion();

        var step = LinkFeatureStepFactory.create(
            "SAME_CATEGORY",
            LinkFeatureStepConfigurationImpl.builder().nodeProperties(List.of("l", "d")).build()
        );
        var linkFeatures = LinkFeatureExtractor.extractFeatures(
            graph,
            List.of(step),
            4,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        assertThat(linkFeatures.get(0)).containsExactly(1, 0);
        assertThat(linkFeatures.get(1)).containsExactly(0, 1);
    }

    @Test
    void failOnArrayProperties() {
        var graph = GdlFactory.of(
            "(a1 {l: 2, da: [0.0, 1.5]})-->(a2 {l: 2L, da: [0.5, 1.5]})"
        ).build().getUnion();

        var step = LinkFeatureStepFactory.create(
            "SAME_CATEGORY",
            LinkFeatureStepConfigurationImpl.builder().nodeProperties(List.of("l", "da")).build()
        );

        assertThatThrownBy(() -> step.linkFeatureAppender(graph))
            .hasMessageContaining("SAME_CATEGORY only supports combining numeric properties, but got node property `da` of type FLOAT_ARRAY.");
    }

}
