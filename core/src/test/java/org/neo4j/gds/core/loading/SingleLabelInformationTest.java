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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SingleLabelInformationTest {

    private static final NodeLabel LABEL_A = NodeLabel.of("A");

    @Test
    void shouldAlwaysBeEmpty() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        var informationEmpty = labelInformation.isEmpty();

        assertThat(informationEmpty).isTrue();
    }

    @Test
    void shouldOnlyContainTheLabelItWasBuiltWith() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        var availableNodeLabels = labelInformation.availableNodeLabels();

        assertThat(availableNodeLabels).containsExactly(LABEL_A);
    }

    @Test
    void shouldReturnItselfWhenFiltered() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        var filteredLabelInformation = labelInformation.filter(List.of(NodeLabel.of("NotLabelA")));
        var filteredNodeLabels = filteredLabelInformation.availableNodeLabels();

        assertThat(filteredLabelInformation).isSameAs(labelInformation);
        assertThat(filteredNodeLabels).containsExactly(LABEL_A);
    }

    @Test
    void hasLabelShouldBeTrueRegardlessOfThePassedNodeId() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());
        var nodeId = new Random().nextLong();

        var hasLabel = labelInformation.hasLabel(nodeId, LABEL_A);

        assertThat(hasLabel).isTrue();
    }

    @Test
    void hasLabelShouldBeFalseForDifferentNodeLabel() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());
        var nodeId = new Random().nextLong();

        var hasLabel = labelInformation.hasLabel(nodeId, NodeLabel.of("NodeLabelA"));

        assertThat(hasLabel).isFalse();
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 3, 19, 42, 1337})
    void nodeCountShouldReturnTheNodeCountWhenTheLabelMatch(long inputNodeCount) {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(inputNodeCount, LongUnaryOperator.identity());

        var nodeCount = labelInformation.nodeCountForLabel(LABEL_A);

        assertThat(nodeCount).isEqualTo(inputNodeCount);
    }

    @Test
    void forEachNodeLabelShouldAcceptNodeLabelConsumer() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());
        var nodeLabelConsumerMock = mock(IdMap.NodeLabelConsumer.class);

        labelInformation.forEachNodeLabel(19, nodeLabelConsumerMock);
        labelInformation.forEachNodeLabel(3, nodeLabelConsumerMock);

        verify(nodeLabelConsumerMock, times(2)).accept(LABEL_A);
    }

    @Test
    void nodeLabelsForNodeIdShouldAlwaysReturnListWithTheSingleNodeLabel() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());
        var nodeId = new Random().nextLong();

        var nodeLabels = labelInformation.nodeLabelsForNodeId(nodeId);

        assertThat(nodeLabels)
            .isInstanceOf(List.class)
            .containsExactly(LABEL_A);
    }

    @Test
    void nodeCountShouldRaiseAnErrorWhenTheLabelDontMatch() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> labelInformation.nodeCountForLabel(NodeLabel.of("NotLabelA")));
    }

    @Test
    void shouldNotFailOnNodeLabel() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        assertThatNoException()
            .isThrownBy(
                () -> labelInformation.validateNodeLabelFilter(List.of(LABEL_A))
            );
    }

    @Test
    void shouldNotAllowForEach() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> labelInformation.forEach((nodeLabel, bitSet) -> false));
    }

    @Test
    void shouldNotAllowUnionBitSet() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> labelInformation.unionBitSet(List.of(), 1337));
    }

    @Test
    void shouldFailOnUnknownNodeLabels() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(
                () -> labelInformation.validateNodeLabelFilter(List.of(NodeLabel.of("NodeLabelA"), NodeLabel.ALL_NODES))
            );
    }

    @Test
    void nodeIteratorShouldWorkForTheCorrectLabel() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        var nodeIterator = labelInformation.nodeIterator(List.of(LABEL_A), 2);

        assertThat(nodeIterator)
            .isNotNull()
            .isInstanceOf(BatchNodeIterable.IdIterator.class);

        var idCounter = new LongAdder();
        nodeIterator.forEachRemaining((LongConsumer) __ -> idCounter.increment());

        assertThat(idCounter.longValue()).isEqualTo(2L);
    }

    @Test
    void nodeIteratorShouldWorkForAllNodesLabel() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        var nodeIterator = labelInformation.nodeIterator(List.of(NodeLabel.ALL_NODES), 2);

        assertThat(nodeIterator)
            .isNotNull()
            .isInstanceOf(BatchNodeIterable.IdIterator.class);

        var idCounter = new LongAdder();
        nodeIterator.forEachRemaining((LongConsumer) __ -> idCounter.increment());

        assertThat(idCounter.longValue()).isEqualTo(2L);
    }

    @Test
    void nodeIteratorShouldFailForIncorrectLabel() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> labelInformation.nodeIterator(List.of(NodeLabel.of("NotLabelA")), 1));
    }

    @Test
    void nodeIteratorShouldFailForMoreThanOneLabel() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() ->
                labelInformation.nodeIterator(
                    List.of(
                        LABEL_A,
                        NodeLabel.ALL_NODES
                    ),
                    1
                ));
    }

    @Test
    void shouldDisallowAddingNodeLabel() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());


        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> labelInformation.addLabel(NodeLabel.of("B")));
    }

    @Test
    void shouldDisallowAddingNodeIdToLabel() {
        var labelInformation = new SingleLabelInformation.Builder(LABEL_A)
            .build(1, LongUnaryOperator.identity());

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> labelInformation.addNodeIdToLabel(LABEL_A, 19L));
    }

    @Nested
    class BuilderTest {
        @Test
        void shouldBuildSingleLabelInformation() {
            var builder = new SingleLabelInformation.Builder(LABEL_A);
            var labelInformation = builder.build(1, LongUnaryOperator.identity());
            assertThat(labelInformation).isNotNull();
        }

        @Test
        void shouldDisallowAddingNodeIdToLabel() {
            var builder = new SingleLabelInformation.Builder(LABEL_A);

            assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> builder.addNodeIdToLabel(LABEL_A, 19L));
        }
    }

}
