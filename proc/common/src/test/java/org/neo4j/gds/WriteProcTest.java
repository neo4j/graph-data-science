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
package org.neo4j.gds;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class WriteProcTest {
    @Test
    void shouldReleaseProgressTrackerAfterCancellation() throws Exception {
        /*
         * we need two mocks.
         *
         * we need to make the delegate business method fail (void write(Collection<NodeProperty> nodeProperties)),
         * and then we can verify that progress tracker is dealt with properly
         */
        var progressTracker = mock(ProgressTracker.class);
        var nodePropertyExporter = mock(NodePropertyExporter.class);

        /*
         * we also need to stub out the generic WriteProc to enable program flow
         *
         * all those generic types? fuhgeddaboudit
         */
        var writeProc = new WriteProc() {
            @Override
            protected AbstractResultBuilder resultBuilder(ComputationResult computeResult) {
                throw new UnsupportedOperationException("TODO");
            }

            @Override
            protected AlgorithmFactory algorithmFactory() {
                throw new UnsupportedOperationException("TODO");
            }

            @Override
            protected AlgoBaseConfig newConfig(
                String username, Optional graphName, Optional maybeImplicitCreate, CypherMapWrapper config
            ) {
                throw new UnsupportedOperationException("TODO");
            }

            @Override
            @NotNull ProgressTracker createProgressTracker(
                Graph graph, ComputationResult computationResult
            ) {
                return progressTracker;
            }

            @Override
            NodePropertyExporter createNodePropertyExporter(
                Graph graph,
                ProgressTracker progressTracker,
                ComputationResult computationResult
            ) {
                return nodePropertyExporter;
            }

            @Override
            protected List<NodeProperty> nodePropertyList(ComputationResult computationResult) {
                return emptyList();
            }
        };

        try {
            // here we set the trap and simulate e.g. cancellation
            doThrow(new RuntimeException("this could be from cancellation or other problems"))
                .when(nodePropertyExporter)
                .write(anyCollection());

            // finally, call write proc and observe the side effects of the exception bubbling through
            writeProc.writeToNeo(new AbstractResultBuilder() {
                @Override
                public Object build() {
                    throw new UnsupportedOperationException("TODO");
                }
            }, new AlgoBaseProc.ComputationResult() {
                @Override
                public long createMillis() {
                    throw new UnsupportedOperationException("TODO");
                }

                @Override
                public long computeMillis() {
                    throw new UnsupportedOperationException("TODO");
                }

                @Nullable
                @Override
                public Algorithm algorithm() {
                    throw new UnsupportedOperationException("TODO");
                }

                @Nullable
                @Override
                public Object result() {
                    throw new UnsupportedOperationException("TODO");
                }

                @Override
                public Graph graph() {
                    return null;
                }

                @Override
                public GraphStore graphStore() {
                    throw new UnsupportedOperationException("TODO");
                }

                @Override
                public AlgoBaseConfig config() {
                    return null;
                }
            });

            // classic pattern, it would be problematic if the exception _did not_ bubble all the way through
            fail();
        } catch (RuntimeException e) {
            // this is fine, exception proceeds up the stack like before
            assertEquals("this could be from cancellation or other problems", e.getMessage());
        }

        // proper resource management :thumbsup:
        verify(progressTracker).beginSubTask();
        verify(progressTracker).endSubTask();
        verify(progressTracker).release();
    }
}
