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
package org.neo4j.gds.similarity.filtering;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.gds.core.loading.ArrayIdMapBuilder;
import org.neo4j.gds.core.loading.LabelInformationBuilders;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class NodeFilterTest {

    @Test
    void shouldFilterBasedOnLabel() {
        // set up an idMap with four nodes and two labels
        var labelOne = NodeLabel.of("one");
        var labelTwo = NodeLabel.of("two");

        var labelInformationBuilder = LabelInformationBuilders.multiLabelWithCapacityAndLabelInformation(4, List.of(labelOne, labelTwo), List.of());
        labelInformationBuilder.addNodeIdToLabel(labelOne, 0);
        labelInformationBuilder.addNodeIdToLabel(labelTwo, 1);
        labelInformationBuilder.addNodeIdToLabel(labelOne, 2);
        labelInformationBuilder.addNodeIdToLabel(labelTwo, 3);

        var arrayIdMapBuilder = ArrayIdMapBuilder.of(4);
        arrayIdMapBuilder.allocate(4);
        var graphIds = arrayIdMapBuilder.array();
        graphIds.set(0, 0);
        graphIds.set(1, 1);
        graphIds.set(2, 2);
        graphIds.set(3, 3);
        var arrayIdMap = arrayIdMapBuilder.build(labelInformationBuilder, 3, 1);

        // test that the idMap is as expected
        assertThat(arrayIdMap.hasLabel(0, labelOne)).isTrue();
        assertThat(arrayIdMap.hasLabel(1, labelTwo)).isTrue();
        assertThat(arrayIdMap.hasLabel(2, labelOne)).isTrue();
        assertThat(arrayIdMap.hasLabel(3, labelTwo)).isTrue();

        // create a node filter based on the idMap
        var nodeFilterOne = LabelNodeFilter.create("one", arrayIdMap);
        var nodeFilterTwo = LabelNodeFilter.create("two", arrayIdMap);

        // test that the filter correctly filters based on the label
        assertThat(nodeFilterOne.test(0)).isTrue();
        assertThat(nodeFilterOne.test(1)).isFalse();
        assertThat(nodeFilterOne.test(2)).isTrue();
        assertThat(nodeFilterOne.test(3)).isFalse();

        assertThat(nodeFilterTwo.test(0)).isFalse();
        assertThat(nodeFilterTwo.test(1)).isTrue();
        assertThat(nodeFilterTwo.test(2)).isFalse();
        assertThat(nodeFilterTwo.test(3)).isTrue();
    }

    @Test
    void shouldFilterBasedOnNodeIds() {
        var nodeFilter = NodeIdNodeFilter.create(Set.of(10L), new DirectIdMap(10));
        assertThat(nodeFilter.test(10)).isTrue();
        assertThat(nodeFilter.test(1)).isFalse();
    }

    @Test
    void noOpFilterShouldReturnTrue() {
        var nodeFilter = NodeFilter.noOp;
        assertThat(nodeFilter.test(10)).isTrue();
        assertThat(nodeFilter.test(1)).isTrue();
    }

}
