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
package org.neo4j.gds.core;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;

import java.util.List;
import java.util.Map;

@ExtendWith(SoftAssertionsExtension.class)
class GraphDimensionsTest {

    @Test
    void shouldBeConstructedCorrectlyWithNodeCount(SoftAssertions assertions) {
        var dimensions = GraphDimensions.of(19);

        assertions.assertThat(dimensions.nodeCount()).isEqualTo(19L);
        assertions.assertThat(dimensions.highestPossibleNodeCount()).isEqualTo(19L);
        assertions.assertThat(dimensions.nodeLabelTokens()).isNull();
        assertions.assertThat(dimensions.relationshipTypeTokens()).isNull();
        assertions.assertThat(dimensions.tokenNodeLabelMapping()).isNull();
        assertions.assertThat(dimensions.relationshipTypeTokenMapping()).isNull();
        assertions.assertThat(dimensions.relationshipCounts()).containsExactly(Map.entry(RelationshipType.ALL_RELATIONSHIPS, 0L));
        assertions.assertThat(dimensions.tokenRelationshipTypeMapping()).isNull();
        assertions.assertThat(dimensions.highestRelationshipId()).isEqualTo(0L);
        assertions.assertThat(dimensions.relCountUpperBound()).isEqualTo(0L);
        assertions.assertThat(dimensions.averageDegree()).isEqualTo(0L);
        assertions.assertThat(dimensions.availableNodeLabels()).isEmpty();
        assertions.assertThat(dimensions.relationshipPropertyTokens()).isEmpty();
        assertions.assertThat(dimensions.estimationNodeLabelCount()).isEqualTo(0L);
        assertions.assertThat(dimensions.starNodeLabelMappings()).isEmpty();
    }

    @Test
    void shouldBeConstructedCorrectlyWithNodeAndRelationshipCounts(SoftAssertions assertions) {
        var dimensions = GraphDimensions.of(19, 33);

        assertions.assertThat(dimensions.nodeCount()).isEqualTo(19L);
        assertions.assertThat(dimensions.highestPossibleNodeCount()).isEqualTo(19L);
        assertions.assertThat(dimensions.nodeLabelTokens()).isNull();
        assertions.assertThat(dimensions.relationshipTypeTokens()).isNull();
        assertions.assertThat(dimensions.tokenNodeLabelMapping()).isNull();
        assertions.assertThat(dimensions.relationshipTypeTokenMapping()).isNull();
        assertions.assertThat(dimensions.relationshipCounts()).containsExactly(Map.entry(RelationshipType.ALL_RELATIONSHIPS, 33L));
        assertions.assertThat(dimensions.tokenRelationshipTypeMapping()).isNull();
        assertions.assertThat(dimensions.highestRelationshipId()).isEqualTo(33L);
        assertions.assertThat(dimensions.relCountUpperBound()).isEqualTo(33L);
        assertions.assertThat(dimensions.averageDegree()).isEqualTo(1L);
        assertions.assertThat(dimensions.availableNodeLabels()).isEmpty();
        assertions.assertThat(dimensions.relationshipPropertyTokens()).isEmpty();
        assertions.assertThat(dimensions.estimationNodeLabelCount()).isEqualTo(0L);
        assertions.assertThat(dimensions.starNodeLabelMappings()).isEmpty();
    }

    @Test
    void shouldNotHaveAvailableNodeLabels(SoftAssertions assertions) {
        IntObjectMap<List<NodeLabel>> emptyTokenNodeLabelMap = new IntObjectHashMap<>();

        var dimensions = GraphDimensions.builder()
            .nodeCount(3)
            .tokenNodeLabelMapping(emptyTokenNodeLabelMap)
            .build();

        assertions.assertThat(dimensions.availableNodeLabels()).isEmpty();
        assertions.assertThat(dimensions.starNodeLabelMappings()).isEmpty();
    }

    @Test
    void shouldHaveCorrectAvailableNodeLabels(SoftAssertions assertions) {
        IntObjectMap<List<NodeLabel>> tokenNodeLabelMap = new IntObjectHashMap<>();
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var labelD = NodeLabel.of("D");
        var labelC = NodeLabel.of("C");
        tokenNodeLabelMap.put(0, List.of(labelA, labelB));
        tokenNodeLabelMap.put(1, List.of(labelC, labelB));
        tokenNodeLabelMap.put(2, List.of(labelA, labelB, labelD));

        var dimensions = GraphDimensions.builder()
            .nodeCount(103)
            .tokenNodeLabelMapping(tokenNodeLabelMap)
            .build();

        assertions.assertThat(dimensions.availableNodeLabels())
            .containsExactlyInAnyOrder(
                labelA, labelB,
                labelC, labelD
            );
        assertions.assertThat(dimensions.starNodeLabelMappings()).isEmpty();
    }

    @Test
    void shouldHaveAvailableNodeLabelsAndStarLabelMapping(SoftAssertions assertions) {
        IntObjectMap<List<NodeLabel>> tokenNodeLabelMap = new IntObjectHashMap<>();
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");
        var labelD = NodeLabel.of("D");
        var labelC = NodeLabel.of("C");
        var starLabel = NodeLabel.of("Star");
        tokenNodeLabelMap.put(-1, List.of(starLabel, labelA));
        tokenNodeLabelMap.put(0, List.of(labelA, labelB));
        tokenNodeLabelMap.put(1, List.of(labelC, labelB));
        tokenNodeLabelMap.put(2, List.of(labelA, labelB, labelD));

        var dimensions = GraphDimensions.builder()
            .nodeCount(19)
            .tokenNodeLabelMapping(tokenNodeLabelMap)
            .build();

        assertions.assertThat(dimensions.availableNodeLabels())
            .containsExactlyInAnyOrder(
                labelA, labelB,
                labelC, labelD,
                starLabel // Q: should this be here?
            );
        assertions.assertThat(dimensions.starNodeLabelMappings()).containsExactlyInAnyOrder(
            labelA,
            starLabel
        );
    }

}
