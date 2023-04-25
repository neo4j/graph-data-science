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
package org.neo4j.gds.similarity.filteredknn;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class FilteredKnnBaseConfigTest {

    @GdlGraph
    static final String GDL = "CREATE (a), (b)";

    @Inject
    GraphStore graphStore;

    @Inject
    IdFunction idFunction;

    @ParameterizedTest
    @MethodSource("configs")
    void shouldAcceptValidSourceNodeFilter(Function<CypherMapWrapper, FilteredKnnBaseConfig> configFunction) {
        assertThatNoException().isThrownBy(
            () ->
                configFunction.apply(
                    CypherMapWrapper.create(
                        Map.of(
                            "nodeProperties", List.of("dummy"),
                            "sourceNodeFilter", List.of(idFunction.of("a"))
                        )
                    )
                ).validateSourceNodeFilter(
                    graphStore,
                    List.of(),
                    List.of()
                )
        );
    }

    @ParameterizedTest
    @MethodSource("configs")
    void shouldAcceptValidTargetNodeFilter(Function<CypherMapWrapper, FilteredKnnBaseConfig> configFunction) {
        assertThatNoException().isThrownBy(
            () ->
                configFunction.apply(
                    CypherMapWrapper.create(
                        Map.of(
                            "nodeProperties", List.of("dummy"),
                            "targetNodeFilter", List.of(idFunction.of("a"))
                        )
                    )
                ).validateTargetNodeFilter(
                    graphStore,
                    List.of(),
                    List.of()
                )
        );
    }

    @ParameterizedTest
    @MethodSource("configs")
    void shouldRejectSourceNodeFilterWithMissingNode(Function<CypherMapWrapper, FilteredKnnBaseConfig> configFunction) {
        var missingNode = missingNode();
        assertThatThrownBy(
            () -> configFunction.apply(
                CypherMapWrapper.create(
                    Map.of(
                        "nodeProperties", List.of("dummy"),
                        "sourceNodeFilter", List.of(idFunction.of("a"), missingNode) // one existing, one missing
                    )
                )
            ).validateSourceNodeFilter(
                graphStore,
                List.of(),
                List.of()
            )
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Invalid configuration value `sourceNodeFilter`, the following nodes are missing from the graph: [" + missingNode + "]");
    }

    @ParameterizedTest
    @MethodSource("configs")
    void shouldRejectTargetNodeFilterWithMissingNode(Function<CypherMapWrapper, FilteredKnnBaseConfig> configFunction) {

        var missingNode = missingNode();
        var userInput = CypherMapWrapper.create(
            Map.of(
                "nodeProperties", List.of("dummy"),
                "targetNodeFilter", List.of(idFunction.of("a"), missingNode) // one existing, one missing
            )
        );

        assertThatThrownBy(
            () -> configFunction.apply(userInput)
                .validateTargetNodeFilter(
                    graphStore,
                    List.of(),
                    List.of()
                )
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Invalid configuration value `targetNodeFilter`, the following nodes are missing from the graph: [" + missingNode + "]");
    }

    @ParameterizedTest
    @MethodSource("configs")
    void shouldRejectSourceNodeFilterWithMissingNodeLabel(Function<CypherMapWrapper, FilteredKnnBaseConfig> configFunction) {

        var userInput = CypherMapWrapper.create(
            Map.of(
                "nodeProperties", List.of("dummy"),
                "sourceNodeFilter", "BogusNodeLabel"
            )
        );

        assertThatThrownBy(
            () -> configFunction.apply(userInput)
                .validateSourceNodeFilter(
                    graphStore,
                    List.of(),
                    List.of()
                )
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Invalid configuration value 'sourceNodeFilter', the node label `BogusNodeLabel` is missing from the graph.");
    }

    @ParameterizedTest
    @MethodSource("configs")
    void shouldRejectTargetNodeFilterWithMissingNodeLabel(Function<CypherMapWrapper, FilteredKnnBaseConfig> configFunction) {

        var missingNode = missingNode();
        var userInput = CypherMapWrapper.create(
            Map.of(
                "nodeProperties", List.of("dummy"),
                "targetNodeFilter", "BogusNodeLabel"
            )
        );

        assertThatThrownBy(
            () -> configFunction.apply(userInput)
                .validateTargetNodeFilter(
                    graphStore,
                    List.of(),
                    List.of()
                )
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Invalid configuration value 'targetNodeFilter', the node label `BogusNodeLabel` is missing from the graph.");
    }


    static Stream<Arguments> configs() {
        return Stream.of(
            Arguments.of((Function<CypherMapWrapper, FilteredKnnBaseConfig>) FilteredKnnStatsConfig::of),
            Arguments.of((Function<CypherMapWrapper, FilteredKnnBaseConfig>) FilteredKnnStreamConfig::of),
            Arguments.of((Function<CypherMapWrapper, FilteredKnnBaseConfig>) (userInput) -> {
                userInput = userInput
                    .withString("mutateRelationshipType", "R")
                    .withString("mutateProperty", "mutateProperty");
                return FilteredKnnMutateConfig.of(userInput);
            }),
            Arguments.of((Function<CypherMapWrapper, FilteredKnnBaseConfig>) (userInput) -> {
                userInput = userInput
                    .withString("writeRelationshipType", "R")
                    .withString("writeProperty", "writeProperty");
                return FilteredKnnWriteConfig.of(userInput);
            })
        );
    }


    private long missingNode() {
        //noinspection OptionalGetWithoutIsPresent
        return new Random()
            .longs(
                0,
                4_294_967_295L // a large-ish number that still fits in our id maps (Math.pow(2, 32) - 1)
            ).filter(l -> !graphStore.nodes().containsOriginalId(l))
            .limit(1)
            .findFirst()
            .getAsLong();
    }
}
