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
package org.neo4j.gds.ml.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineCatalogTest {

    public static final String ALICE = "alice";

    @AfterEach
    void cleanCatalog() {
        PipelineCatalog.removeAll();
    }

    @Test
    void setPipeline() {
        var alicesPipeline = new NodeClassificationTrainingPipeline();
        PipelineCatalog.set(ALICE, "myPipe", alicesPipeline);

        assertThat(PipelineCatalog.getAllPipelines(ALICE))
            .containsExactly(ImmutablePipelineCatalogEntry.of("myPipe", alicesPipeline));


        assertThat(PipelineCatalog.getAllPipelines("bob")).isEmpty();

        LinkPredictionTrainingPipeline bobsPipeline = new LinkPredictionTrainingPipeline();
        PipelineCatalog.set("bob", "myPipe", bobsPipeline);

        assertThat(PipelineCatalog.getAllPipelines(ALICE)).containsExactly(
            ImmutablePipelineCatalogEntry.of("myPipe", alicesPipeline)
        );

        assertThat(PipelineCatalog.getAllPipelines("bob")).containsExactly(
            ImmutablePipelineCatalogEntry.of("myPipe", bobsPipeline)
        );
    }

    @Test
    void failOnSettingExistingPipeline() {
        PipelineCatalog.set(ALICE, "myPipe", new NodeClassificationTrainingPipeline());

        assertThatThrownBy(() -> PipelineCatalog.set(ALICE, "myPipe", new NodeClassificationTrainingPipeline()))
            .hasMessage("Pipeline named `myPipe` already exists.");
    }

    @Test
    void getPipeline() {
        var onePipe = new NodeClassificationTrainingPipeline();
        var otherPipe = new LinkPredictionTrainingPipeline();
        PipelineCatalog.set(ALICE, "onePipe", onePipe);
        PipelineCatalog.set(ALICE, "otherPipe", otherPipe);

        assertThat(PipelineCatalog.get(ALICE, "onePipe")).isSameAs(onePipe);
        assertThat(PipelineCatalog.get(ALICE, "otherPipe")).isSameAs(otherPipe);
    }

    @Test
    void getTypedPipeline() {
        var ncPipe = new NodeClassificationTrainingPipeline();
        var lpPipe = new LinkPredictionTrainingPipeline();
        PipelineCatalog.set(ALICE, "onePipe", ncPipe);
        PipelineCatalog.set(ALICE, "lpPipe", lpPipe);

        assertThat(PipelineCatalog.getTyped(ALICE, "onePipe", NodeClassificationTrainingPipeline.class)).isSameAs(ncPipe);
        assertThat(PipelineCatalog.getTyped(ALICE, "lpPipe", LinkPredictionTrainingPipeline.class)).isSameAs(lpPipe);
    }

    @Test
    void failOnGetNonExistingPipeline() {
        assertThatThrownBy(() -> PipelineCatalog.get(ALICE, "NOT_SAVED_PIPE"))
            .hasMessage("Pipeline with name `NOT_SAVED_PIPE` does not exist for user `alice`.");
    }

    @Test
    void failOnGetTypedOnUnexpectedTypedPipeline() {
        PipelineCatalog.set(ALICE, "ncPipe", new NodeClassificationTrainingPipeline());
        assertThatThrownBy(() -> PipelineCatalog.getTyped(ALICE, "ncPipe", LinkPredictionTrainingPipeline.class))
            .hasMessage("The pipeline `ncPipe` is of type `Node classification training pipeline`, but expected type `Link prediction training pipeline`.");
    }

    @Test
    void exists() {
        assertThat(PipelineCatalog.exists(ALICE, "onePipe")).isFalse();

        PipelineCatalog.set(ALICE, "onePipe", new NodeClassificationTrainingPipeline());

        assertThat(PipelineCatalog.exists(ALICE, "onePipe")).isTrue();
    }

    @Test
    void dropPipeline() {
        NodeClassificationTrainingPipeline onePipe = new NodeClassificationTrainingPipeline();
        PipelineCatalog.set(ALICE, "onePipe", onePipe);

        assertThat(PipelineCatalog.drop(ALICE, "onePipe")).isSameAs(onePipe);
    }

    @Test
    void failOnDropNonExistingPipeline() {
        assertThatThrownBy(() -> PipelineCatalog.drop(ALICE, "NOT_SAVED_PIPE"))
            .hasMessage("Pipeline with name `NOT_SAVED_PIPE` does not exist for user `alice`.");
    }
}
