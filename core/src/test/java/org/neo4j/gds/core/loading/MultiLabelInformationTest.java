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
package org.neo4j.gds.core.loading;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.BatchNodeIterable;
import org.neo4j.gds.api.NodeIdMapper;
import org.neo4j.gds.api.nodes.LabelInformation;
import org.neo4j.gds.api.nodes.NodeLabelConsumer;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(SoftAssertionsExtension.class)
class MultiLabelInformationTest {

    @Test
    void shouldNotBeSingleLabel() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());

        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        assertThat(labelInformation.isSingleLabel()).isFalse();
    }

    @Test
    void shouldBeEmptyWhenThereIsNoLabelInformation() {
        var builder = MultiLabelInformation.Builder.of(19, List.of(), List.of());
        var labelInformation = builder.build(19, NodeIdMapper.IDENTITY);

        assertThat(labelInformation.isEmpty()).isTrue();
    }

    @Test
    void shouldNotBeEmptyWhenThereIsLabelInformation() {
        var builder = MultiLabelInformation.Builder.of(
            19,
            // Implementation goes down to `SingleLabelInformation` if there is only one label in the availableLabels
            List.of(NodeLabel.of("A"), NodeLabel.of("B")),
            List.of()
        );
        var labelInformation = builder.build(19, NodeIdMapper.IDENTITY);

        assertThat(labelInformation.isEmpty()).isFalse();
    }

    @Test
    void shouldNotBeEmptyWhenThereIsNoLabelInformationButHasStarLabels() {
        var builder = MultiLabelInformation.Builder.of(19, List.of(), List.of(NodeLabel.of("B")));
        var labelInformation = builder.build(19, NodeIdMapper.IDENTITY);

        assertThat(labelInformation.isEmpty()).isFalse();
    }

    @Test
    void shouldAcceptLabelInformationConsumerForEachLabel() {
        var builder = MultiLabelInformation.Builder.of(
            1,
            // Implementation goes down to `SingleLabelInformation` if there is only one label in the availableLabels
            List.of(NodeLabel.of("A"), NodeLabel.of("B")),
            List.of()
        );
        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        var labelInformationConsumerMock = mock(LabelInformation.LabelInformationConsumer.class);
        when(labelInformationConsumerMock.accept(any(), any())).thenReturn(true, true, false);

        labelInformation.forEach(labelInformationConsumerMock);

        verify(labelInformationConsumerMock, times(1)).accept(eq(NodeLabel.of("A")), any());
        verify(labelInformationConsumerMock, times(1)).accept(eq(NodeLabel.of("B")), any());
    }

    @Test
    void identityBuildKeepsLabelsForMappedIds() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());
        // ids spanning multiple 64-bit words and beyond a single page
        var aIds = List.of(0L, 1L, 63L, 64L, 130L);
        var bIds = List.of(2L, 200L);
        aIds.forEach(id -> builder.addNodeIdToLabel(labelA, id));
        bIds.forEach(id -> builder.addNodeIdToLabel(labelB, id));

        var labelInformation = builder.build(256, NodeIdMapper.IDENTITY);

        aIds.forEach(id -> assertThat(labelInformation.hasLabel(id, labelA)).as("A has %d", id).isTrue());
        bIds.forEach(id -> assertThat(labelInformation.hasLabel(id, labelB)).as("B has %d", id).isTrue());
        assertThat(labelInformation.nodeCountForLabel(labelA)).isEqualTo(aIds.size());
        assertThat(labelInformation.nodeCountForLabel(labelB)).isEqualTo(bIds.size());
        assertThat(labelInformation.hasLabel(0L, labelB)).isFalse();
    }

    @Test
    void identityFastPathMatchesRemapPath() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        long nodeCount = 300;

        var fast = builderWithLabels(labelA, labelB).build(nodeCount, NodeIdMapper.IDENTITY);
        var slow = builderWithLabels(labelA, labelB).build(nodeCount, id -> id);

        for (long id = 0; id < nodeCount; id++) {
            assertThat(fast.hasLabel(id, labelA)).as("label A, node %d", id).isEqualTo(slow.hasLabel(id, labelA));
            assertThat(fast.hasLabel(id, labelB)).as("label B, node %d", id).isEqualTo(slow.hasLabel(id, labelB));
        }
        assertThat(fast.nodeCountForLabel(labelA)).isEqualTo(slow.nodeCountForLabel(labelA));
        assertThat(fast.nodeCountForLabel(labelB)).isEqualTo(slow.nodeCountForLabel(labelB));
    }

    @Test
    void nonIdentityBuildRemapsLabelsToMappedIds() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());

        // The import bit sets are keyed by sparse original ids ...
        builder.addNodeIdToLabel(labelA, 10L);
        builder.addNodeIdToLabel(labelA, 20L);
        builder.addNodeIdToLabel(labelA, 30L);
        builder.addNodeIdToLabel(labelB, 40L);
        builder.addNodeIdToLabel(labelB, 50L);

        // ... that are remapped into a dense [0, 5) mapped id space.
        var originalToMapped = new long[51];
        originalToMapped[10] = 0L;
        originalToMapped[20] = 1L;
        originalToMapped[30] = 2L;
        originalToMapped[40] = 3L;
        originalToMapped[50] = 4L;
        NodeIdMapper mappedIdFn = originalId -> originalToMapped[(int) originalId];

        var labelInformation = builder.build(5, mappedIdFn);

        // Labels live at the mapped ids, not the original ones.
        assertThat(labelInformation.hasLabel(0L, labelA)).isTrue();
        assertThat(labelInformation.hasLabel(1L, labelA)).isTrue();
        assertThat(labelInformation.hasLabel(2L, labelA)).isTrue();
        assertThat(labelInformation.hasLabel(3L, labelB)).isTrue();
        assertThat(labelInformation.hasLabel(4L, labelB)).isTrue();

        assertThat(labelInformation.hasLabel(0L, labelB)).isFalse();
        assertThat(labelInformation.hasLabel(3L, labelA)).isFalse();

        assertThat(labelInformation.nodeCountForLabel(labelA)).isEqualTo(3L);
        assertThat(labelInformation.nodeCountForLabel(labelB)).isEqualTo(2L);
    }

    @Test
    void buildRemapsLabelsWhenMappedIdSpaceIsReversed() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());

        long nodeCount = 128;
        // Assign labels by original id, then reverse the id space on build.
        for (long id = 0; id < nodeCount; id++) {
            builder.addNodeIdToLabel(id % 2 == 0 ? labelA : labelB, id);
        }
        NodeIdMapper reverse = originalId -> nodeCount - 1 - originalId;

        var labelInformation = builder.build(nodeCount, reverse);

        for (long id = 0; id < nodeCount; id++) {
            var expectedLabel = id % 2 == 0 ? labelA : labelB;
            long mappedId = nodeCount - 1 - id;
            assertThat(labelInformation.hasLabel(mappedId, expectedLabel))
                .as("original %d -> mapped %d should carry %s", id, mappedId, expectedLabel)
                .isTrue();
        }
        assertThat(labelInformation.nodeCountForLabel(labelA)).isEqualTo(nodeCount / 2);
        assertThat(labelInformation.nodeCountForLabel(labelB)).isEqualTo(nodeCount / 2);
    }

    private static MultiLabelInformation.Builder builderWithLabels(NodeLabel labelA, NodeLabel labelB) {
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());
        for (long id = 0; id < 300; id += 3) {
            builder.addNodeIdToLabel(labelA, id);
        }
        for (long id = 1; id < 300; id += 7) {
            builder.addNodeIdToLabel(labelB, id);
        }
        return builder;
    }

    @Test
    void shouldFilterByNodeLabels() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());
        builder.addNodeIdToLabel(labelA, 1L);
        builder.addNodeIdToLabel(labelB, 2L);
        builder.addNodeIdToLabel(labelB, 3L);

        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        var filteredLabelInformation = labelInformation.filter(List.of(labelA), 4, NodeIdMapper.IDENTITY);

        assertThat(filteredLabelInformation.availableNodeLabels()).containsExactly(labelA);
    }

    @Test
    void filteredLabelInformationShouldNotReportOnParentLabels() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());
        builder.addNodeIdToLabel(labelA, 1L);
        builder.addNodeIdToLabel(labelB, 2L);
        builder.addNodeIdToLabel(labelB, 3L);

        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        var filteredLabelInformation = labelInformation.filter(List.of(labelA), 4, NodeIdMapper.IDENTITY);

        assertThat(filteredLabelInformation.nodeLabelsForNodeId(1L)).containsExactly(labelA);

        assertThat(filteredLabelInformation.nodeLabelsForNodeId(2L)).isEmpty();
        assertThat(filteredLabelInformation.nodeLabelsForNodeId(3L)).isEmpty();
    }

    @Test
    @Disabled("This test throws NPE, enable that once the question at the end of this method is answered and the implementation fixed. Don't forget to add the correct Assertions!")
    void whatShouldHappenIfWeFilterByUnknownLabels() {
        var builder = MultiLabelInformation.Builder.of(
            1,
            List.of(NodeLabel.of("A"), NodeLabel.of("B")),
            List.of()
        );
        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        // Here we get NPE, because...well we don't check if the labels we try to filter by actually exist.
        var filteredLabelInformation = labelInformation.filter(List.of(NodeLabel.of("C")), 1, NodeIdMapper.IDENTITY);

        // TODO: What is the expected behaviour?
    }

    @Test
    void shouldReturnTheCorrectNodeCountForLabel() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());
        builder.addNodeIdToLabel(labelA, 1L);
        builder.addNodeIdToLabel(labelB, 2L);
        builder.addNodeIdToLabel(labelB, 3L);

        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        assertThat(labelInformation.nodeCountForLabel(labelA)).isEqualTo(1L);
        assertThat(labelInformation.nodeCountForLabel(labelB)).isEqualTo(2L);
    }

    @Test
    void shouldRaiseAnErrorIfTryingToLookForCountsOfNonExistingLabels() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());
        builder.addNodeIdToLabel(labelA, 1L);
        builder.addNodeIdToLabel(labelB, 2L);
        builder.addNodeIdToLabel(labelB, 3L);

        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> labelInformation.nodeCountForLabel(NodeLabel.of("U")));
    }

    @Test
    void hasLabelShouldBeTrueForAllNodesLabel() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        var nodeId = new Random().nextLong();

        var labelInformation = builder.build(3, NodeIdMapper.IDENTITY);

        assertThat(labelInformation.hasLabel(nodeId, NodeLabel.ALL_NODES)).isTrue();
    }

    @Test
    void hasLabelShouldBeTrueForKnownLabel() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());
        builder.addNodeIdToLabel(labelA, 1L);
        builder.addNodeIdToLabel(labelB, 2L);
        builder.addNodeIdToLabel(labelB, 3L);

        var labelInformation = builder.build(3, NodeIdMapper.IDENTITY);

        assertThat(labelInformation.hasLabel(1L, labelA)).isTrue();
        assertThat(labelInformation.hasLabel(2L, labelB)).isTrue();
        assertThat(labelInformation.hasLabel(3L, labelB)).isTrue();
    }

    @Test
    void hasLabelShouldBeFalseForUnknownLabel() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        builder.addNodeIdToLabel(NodeLabel.of("A"), 1L);
        builder.addNodeIdToLabel(NodeLabel.of("B"), 2L);
        builder.addNodeIdToLabel(NodeLabel.of("B"), 3L);

        var labelInformation = builder.build(3, NodeIdMapper.IDENTITY);

        assertThat(labelInformation.hasLabel(1L, NodeLabel.of("C"))).isFalse();
        assertThat(labelInformation.hasLabel(2L, NodeLabel.of("D"))).isFalse();
        assertThat(labelInformation.hasLabel(3L, NodeLabel.of("E"))).isFalse();
    }

    @Test
    void shouldHaveCorrectUnionBitSet() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        builder.addNodeIdToLabel(NodeLabel.of("A"), 1L);
        builder.addNodeIdToLabel(NodeLabel.of("B"), 2L);
        builder.addNodeIdToLabel(NodeLabel.of("B"), 3L);
        builder.addNodeIdToLabel(NodeLabel.of("C"), 4L);

        var labelInformation = builder.build(3, NodeIdMapper.IDENTITY);

        var unionBitSet = labelInformation.unionBitSet(List.of(NodeLabel.of("C"), NodeLabel.of("A")), 4);

        assertThat(unionBitSet.get(1)).isTrue();
        assertThat(unionBitSet.get(2)).isFalse();
        assertThat(unionBitSet.get(3)).isFalse();
        assertThat(unionBitSet.get(4)).isTrue();
    }

    @Test
    void shouldValidateSuccessfully() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        assertThatNoException()
            .isThrownBy(
                () -> labelInformation.validateNodeLabelFilter(List.of(NodeLabel.of("A")))
            );
    }

    @Test
    void shouldFailOnUnknownNodeLabels() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(
                () -> labelInformation.validateNodeLabelFilter(List.of(NodeLabel.of("NodeLabelA"), NodeLabel.ALL_NODES))
            );
    }

    @Test
    void forEachNodeLabelShouldWorkAsExpected() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        builder.addNodeIdToLabel(NodeLabel.of("A"), 1L);
        builder.addNodeIdToLabel(NodeLabel.of("B"), 1L);
        builder.addNodeIdToLabel(NodeLabel.of("B"), 2L);
        builder.addNodeIdToLabel(NodeLabel.of("B"), 3L);

        var labelInformation = builder.build(3, NodeIdMapper.IDENTITY);

        var nodeLabelConsumerMock = mock(NodeLabelConsumer.class);
        when(nodeLabelConsumerMock.accept(any())).thenReturn(true);

        labelInformation.forEachNodeLabel(1L, nodeLabelConsumerMock);
        labelInformation.forEachNodeLabel(2L, nodeLabelConsumerMock);
        labelInformation.forEachNodeLabel(3L, nodeLabelConsumerMock);

        verify(nodeLabelConsumerMock, times(1)).accept(NodeLabel.of("A"));
        verify(nodeLabelConsumerMock, times(3)).accept(NodeLabel.of("B"));
    }

    @Test
    void nodeIteratorShouldWorkForKnownLabels() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B"), NodeLabel.of("C")), List.of());
        builder.addNodeIdToLabel(NodeLabel.of("A"), 1L);
        builder.addNodeIdToLabel(NodeLabel.of("B"), 2L);
        builder.addNodeIdToLabel(NodeLabel.of("C"), 3L);
        builder.addNodeIdToLabel(NodeLabel.of("A"), 4L);
        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        var nodeIterator = labelInformation.nodeIterator(List.of(NodeLabel.of("A"), NodeLabel.of("C")), 4);

        assertThat(nodeIterator)
            .isNotNull()
            .isInstanceOf(BatchNodeIterable.BitSetIdIterator.class);

        var idCounter = new LongAdder();
        nodeIterator.forEachRemaining((LongConsumer) __ -> idCounter.increment());

        assertThat(idCounter.longValue()).isEqualTo(3L);
    }

    @Test
    void nodeIteratorShouldWorkForAllNodesLabel() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        var nodeIterator = labelInformation.nodeIterator(List.of(NodeLabel.ALL_NODES), 2);

        assertThat(nodeIterator)
            .isNotNull()
            .isInstanceOf(BatchNodeIterable.IdIterator.class);

        var idCounter = new LongAdder();
        nodeIterator.forEachRemaining((LongConsumer) __ -> idCounter.increment());

        assertThat(idCounter.longValue()).isEqualTo(2L);
    }


    @Test
    void shouldAddNodeLabel(SoftAssertions assertions) {
        // Arrange
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");

        var availableNodeLabels = List.of(labelA, labelB);

        var builder = LabelInformationBuilders
            .multiLabelWithCapacityAndLabelInformation(42, availableNodeLabels, Collections.emptyList());

        // Act
        var newLabel = NodeLabel.of("C");
        var labelInformation = builder.build(42, NodeIdMapper.IDENTITY);
        labelInformation.addLabel(newLabel);

        // Assert
        assertions.assertThat(labelInformation.availableNodeLabels())
            .as("The new label `C` should appear in the available node labels.")
            .containsExactlyInAnyOrder(
                labelA,
                labelB,
                newLabel
            );

        // Simply adding the node label to the information should not associate it with any nodes
        assertions.assertThat(labelInformation.nodeCountForLabel(newLabel))
            .as("The new label `C` should not be mapped to any nodes")
            .isEqualTo(0L);
    }

    @Test
    void shouldAssignNodeLabelToNodes(SoftAssertions assertions) {

        // Arrange
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");

        var availableNodeLabels = List.of(labelA, labelB);
        var builder = LabelInformationBuilders
            .multiLabelWithCapacityAndLabelInformation(42, availableNodeLabels, Collections.emptyList());

        var labelInformation = builder.build(42, NodeIdMapper.IDENTITY);
        var newLabel = NodeLabel.of("C");
        labelInformation.addLabel(newLabel);

        // Act
        labelInformation.addNodeIdToLabel(2, newLabel);

        // Assert

        assertions.assertThat(labelInformation.nodeCountForLabel(newLabel))
            .as("The newly added label `C` should be mapped to exactly one node")
            .isEqualTo(1L);

        assertions.assertThat(labelInformation.nodeLabelsForNodeId(2))
            .as("Node with ID `2` should be mapped to the new label `C`")
            .containsExactlyInAnyOrder(
                newLabel
            );

        assertions.assertThat(labelInformation.hasLabel(2, newLabel))
            .as("Node with ID `2` should have the new label `C`")
            .isTrue();
    }

    @Test
    void shouldAddTheLabelAndReturnItselfWhenConvertedToMultiLabel() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());

        var labelInformation = builder.build(1, NodeIdMapper.IDENTITY);

        var labelC = NodeLabel.of("C");

        var multiLabelInformation = labelInformation.toMultiLabel(labelC);

        assertThat(multiLabelInformation).isSameAs(labelInformation);
        assertThat(multiLabelInformation.isSingleLabel()).isFalse();

        assertThat(multiLabelInformation.availableNodeLabels()).containsExactlyInAnyOrder(
            labelA,
            labelB,
            labelC
        );
    }

    @Nested
    class BuilderTest {

        @Test
        void shouldBuildLabelInformationWithCapacity() {
            var builder = MultiLabelInformation.Builder.of(2);
            var labelInformation = builder.build(2, NodeIdMapper.IDENTITY);

            assertThat(labelInformation).isNotNull();
            assertThat(labelInformation.isEmpty()).isTrue();
            assertThat(labelInformation.availableNodeLabels()).containsExactly(NodeLabel.ALL_NODES);
        }

        @Test
        void shouldBuildLabelInformationWithCapacityAndEmptyLabelInformation() {
            var builder = MultiLabelInformation.Builder.of(19, List.of(), List.of());
            var labelInformation = builder.build(19, NodeIdMapper.IDENTITY);

            assertThat(labelInformation).isNotNull();
            assertThat(labelInformation.isEmpty()).isTrue();
            assertThat(labelInformation.availableNodeLabels()).containsExactly(NodeLabel.ALL_NODES);
        }

        @Test
        void shouldBuildLabelInformationWithCapacityAndStarLabelInformation() {
            var builder = MultiLabelInformation.Builder.of(21, List.of(), List.of(NodeLabel.of("Star")));
            var labelInformation = builder.build(21, NodeIdMapper.IDENTITY);

            assertThat(labelInformation).isNotNull();
            assertThat(labelInformation.isEmpty()).isFalse();
            assertThat(labelInformation.availableNodeLabels()).containsExactly(NodeLabel.of("Star"));
        }

        @Test
        void shouldAddNodeIdsToLabel() {
            var builder = MultiLabelInformation.Builder.of(2, List.of(), List.of());

            builder.addNodeIdToLabel(NodeLabel.of("A"), 1L);
            builder.addNodeIdToLabel(NodeLabel.of("B"), 2L);
            builder.addNodeIdToLabel(NodeLabel.of("B"), 3L);

            // TODO: Figure out why we get the three nodes when we build the `MultiLabelInformation` with `nodeCount=2`
            var labelInformation = builder.build(2, NodeIdMapper.IDENTITY);

            assertThat(labelInformation.availableNodeLabels())
                .containsExactlyInAnyOrder(NodeLabel.of("A"), NodeLabel.of("B"));

            assertThat(labelInformation.nodeCountForLabel(NodeLabel.of("A"))).isEqualTo(1L);
            assertThat(labelInformation.nodeCountForLabel(NodeLabel.of("B"))).isEqualTo(2L);

            assertThat(labelInformation.nodeLabelsForNodeId(1L)).contains(NodeLabel.of("A"));
            assertThat(labelInformation.nodeLabelsForNodeId(2L)).contains(NodeLabel.of("B"));
            assertThat(labelInformation.nodeLabelsForNodeId(3L)).contains(NodeLabel.of("B"));
        }


        @Test
        void shouldAcceptConcurrentInserts() {
            var builder = MultiLabelInformation.Builder.of(110, List.of(), List.of());

            // Create 10 tasks that try to insert overlapping node labels
             List<Runnable> tasks = LongStream
                 .range(0, 10)
                 .mapToObj(i ->
                     (Runnable) () -> LongStream.range(i * 10, i * 10 + 20).forEach(l -> builder.addNodeIdToLabel(NodeLabel.of("" + l), l))
                 ).collect(toList());

            RunWithConcurrency.builder()
                .tasks(tasks)
                .concurrency(new Concurrency(4))
                .build()
                .run();

            var map = builder.build(110, i -> i);

            LongStream.range(0, 110).forEach(i -> assertThat(map.hasLabel(i, NodeLabel.of("" + i))).isTrue());
        }

    }
}
