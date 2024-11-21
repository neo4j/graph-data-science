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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.progress.ThreadSafeMockingProgress;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.core.loading.PostLoadValidationHook;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class NodeEmbeddingAlgorithmsBusinessFacadeTest {
    @Test
    void shouldValidateNode2VecDataSizeInMutateMode() {
        var processingTemplate = mock(AlgorithmProcessingTemplateConvenience.class);
        var facade = new NodeEmbeddingAlgorithmsMutateModeBusinessFacade(null, null, processingTemplate, null, null);

        facade.node2Vec(null, null, null);

        verify(processingTemplate).processAlgorithmInMutateMode(
            any(),
            any(),
            any(),
            node2VecValidationHook(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any()
        );
    }

    @Test
    void shouldValidateNode2VecDataSizeInStreamMode() {
        var processingTemplate = mock(AlgorithmProcessingTemplateConvenience.class);
        var facade = new NodeEmbeddingAlgorithmsStreamModeBusinessFacade(null, null, processingTemplate, null);

        facade.node2Vec(null, null, null);

        verify(processingTemplate).processAlgorithmInStreamMode(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            node2VecValidationHook(),
            any(),
            any()
        );
    }

    @Test
    void shouldValidateNode2VecDataSizeInWriteMode() {
        var processingTemplate = mock(AlgorithmProcessingTemplateConvenience.class);
        var facade = new NodeEmbeddingAlgorithmsWriteModeBusinessFacade(null, null, processingTemplate, null, null);

        facade.node2Vec(null, null, null);

        verify(processingTemplate).processAlgorithmInWriteMode(
            any(),
            any(),
            any(),
            node2VecValidationHook(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any()
        );
    }

    /**
     * Verify that you did indeed stack on the right validation hook
     */
    private static Optional<Iterable<PostLoadValidationHook>> node2VecValidationHook() {
        ThreadSafeMockingProgress.mockingProgress()
            .getArgumentMatcherStorage()
            .reportMatcher((ArgumentMatcher<Optional<Iterable<PostLoadValidationHook>>>) argument -> {
                assertThat(argument.orElseThrow()).singleElement().isInstanceOf(Node2VecValidationHook.class);
                return true;
            });
        //noinspection DataFlowIssue,OptionalAssignedToNull
        return null;
    }
}
