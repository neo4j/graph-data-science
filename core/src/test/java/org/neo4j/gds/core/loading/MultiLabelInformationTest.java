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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.BatchNodeIterable;
import org.neo4j.gds.api.IdMap;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MultiLabelInformationTest {

    @Test
    void shouldBeEmptyWhenThereIsNoLabelInformation() {
        var builder = MultiLabelInformation.Builder.of(19, List.of(), List.of());
        var labelInformation = builder.build(19, LongUnaryOperator.identity());

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
        var labelInformation = builder.build(19, LongUnaryOperator.identity());

        assertThat(labelInformation.isEmpty()).isFalse();
    }

    @Test
    void shouldNotBeEmptyWhenThereIsNoLabelInformationButHasStarLabels() {
        var builder = MultiLabelInformation.Builder.of(19, List.of(), List.of(NodeLabel.of("B")));
        var labelInformation = builder.build(19, LongUnaryOperator.identity());

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
        var labelInformation = builder.build(1, LongUnaryOperator.identity());

        var labelInformationConsumerMock = mock(LabelInformation.LabelInformationConsumer.class);
        when(labelInformationConsumerMock.accept(any(), any())).thenReturn(true, true, false);

        labelInformation.forEach(labelInformationConsumerMock);

        verify(labelInformationConsumerMock, times(1)).accept(eq(NodeLabel.of("A")), any());
        verify(labelInformationConsumerMock, times(1)).accept(eq(NodeLabel.of("B")), any());
    }

    @Test
    void shouldFilterByNodeLabels() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var builder = MultiLabelInformation.Builder.of(1, List.of(labelA, labelB), List.of());
        builder.addNodeIdToLabel(labelA, 1L);
        builder.addNodeIdToLabel(labelB, 2L);
        builder.addNodeIdToLabel(labelB, 3L);

        var labelInformation = builder.build(1, LongUnaryOperator.identity());

        var filteredLabelInformation = labelInformation.filter(List.of(labelA));

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

        var labelInformation = builder.build(1, LongUnaryOperator.identity());

        var filteredLabelInformation = labelInformation.filter(List.of(labelA));

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
        var labelInformation = builder.build(1, LongUnaryOperator.identity());

        // Here we get NPE, because...well we don't check if the labels we try to filter by actually exist.
        var filteredLabelInformation = labelInformation.filter(List.of(NodeLabel.of("C")));

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

        var labelInformation = builder.build(1, LongUnaryOperator.identity());

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

        var labelInformation = builder.build(1, LongUnaryOperator.identity());

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> labelInformation.nodeCountForLabel(NodeLabel.of("U")));
    }

    @Test
    void hasLabelShouldBeTrueForAllNodesLabel() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        var nodeId = new Random().nextLong();

        var labelInformation = builder.build(3, LongUnaryOperator.identity());

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

        var labelInformation = builder.build(3, LongUnaryOperator.identity());

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

        var labelInformation = builder.build(3, LongUnaryOperator.identity());

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

        var labelInformation = builder.build(3, LongUnaryOperator.identity());

        var unionBitSet = labelInformation.unionBitSet(List.of(NodeLabel.of("C"), NodeLabel.of("A")), 4);

        assertThat(unionBitSet.get(1)).isTrue();
        assertThat(unionBitSet.get(2)).isFalse();
        assertThat(unionBitSet.get(3)).isFalse();
        assertThat(unionBitSet.get(4)).isTrue();
    }

    @Test
    void shouldValidateSuccessfully() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        var labelInformation = builder.build(1, LongUnaryOperator.identity());

        assertThatNoException()
            .isThrownBy(
                () -> labelInformation.validateNodeLabelFilter(List.of(NodeLabel.of("A")))
            );
    }

    @Test
    void shouldFailOnUnknownNodeLabels() {
        var builder = MultiLabelInformation.Builder.of(1, List.of(NodeLabel.of("A"), NodeLabel.of("B")), List.of());
        var labelInformation = builder.build(1, LongUnaryOperator.identity());

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

        var labelInformation = builder.build(3, LongUnaryOperator.identity());

        var nodeLabelConsumerMock = mock(IdMap.NodeLabelConsumer.class);
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
        var labelInformation = builder.build(1, LongUnaryOperator.identity());

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
        var labelInformation = builder.build(1, LongUnaryOperator.identity());

        var nodeIterator = labelInformation.nodeIterator(List.of(NodeLabel.ALL_NODES), 2);

        assertThat(nodeIterator)
            .isNotNull()
            .isInstanceOf(BatchNodeIterable.IdIterator.class);

        var idCounter = new LongAdder();
        nodeIterator.forEachRemaining((LongConsumer) __ -> idCounter.increment());

        assertThat(idCounter.longValue()).isEqualTo(2L);
    }

    @Nested
    class BuilderTest {

        @Test
        void shouldBuildLabelInformationWithCapacity() {
            var builder = MultiLabelInformation.Builder.of(2);
            var labelInformation = builder.build(2, LongUnaryOperator.identity());

            assertThat(labelInformation).isNotNull();
            assertThat(labelInformation.isEmpty()).isTrue();
            assertThat(labelInformation.availableNodeLabels()).containsExactly(NodeLabel.ALL_NODES);
        }

        @Test
        void shouldBuildLabelInformationWithCapacityAndEmptyLabelInformation() {
            var builder = MultiLabelInformation.Builder.of(19, List.of(), List.of());
            var labelInformation = builder.build(19, LongUnaryOperator.identity());

            assertThat(labelInformation).isNotNull();
            assertThat(labelInformation.isEmpty()).isTrue();
            assertThat(labelInformation.availableNodeLabels()).containsExactly(NodeLabel.ALL_NODES);
        }

        @Test
        void shouldBuildLabelInformationWithCapacityAndStarLabelInformation() {
            var builder = MultiLabelInformation.Builder.of(21, List.of(), List.of(NodeLabel.of("Star")));
            var labelInformation = builder.build(21, LongUnaryOperator.identity());

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
            var labelInformation = builder.build(2, LongUnaryOperator.identity());

            assertThat(labelInformation.availableNodeLabels())
                .containsExactlyInAnyOrder(NodeLabel.of("A"), NodeLabel.of("B"));

            assertThat(labelInformation.nodeCountForLabel(NodeLabel.of("A"))).isEqualTo(1L);
            assertThat(labelInformation.nodeCountForLabel(NodeLabel.of("B"))).isEqualTo(2L);

            assertThat(labelInformation.nodeLabelsForNodeId(1L)).contains(NodeLabel.of("A"));
            assertThat(labelInformation.nodeLabelsForNodeId(2L)).contains(NodeLabel.of("B"));
            assertThat(labelInformation.nodeLabelsForNodeId(3L)).contains(NodeLabel.of("B"));
        }

    }
}
