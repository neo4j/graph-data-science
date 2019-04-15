/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.similarity;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphalgo.similarity.SimilarityVectorAggregator.CATEGORY_KEY;
import static org.neo4j.graphalgo.similarity.SimilarityVectorAggregator.WEIGHT_KEY;

public class SimilarityVectorAggregatorTest {

    @Test
    public void singleItem() {
        SimilarityVectorAggregator aggregator = new SimilarityVectorAggregator();

        Node node = mock(Node.class);
        when(node.getId()).thenReturn(1L);

        aggregator.next(node, 3.0);

        List<Map<String, Object>> expected = Collections.singletonList(
                MapUtil.map(CATEGORY_KEY, 1L, WEIGHT_KEY, 3.0)
        );

        assertThat(aggregator.result(), is(expected));
    }

    @Test
    public void multipleItems() {
        SimilarityVectorAggregator aggregator = new SimilarityVectorAggregator();

        Node node = mock(Node.class);
        when(node.getId()).thenReturn(1L, 2L, 3L);

        aggregator.next(node, 3.0);
        aggregator.next(node, 2.0);
        aggregator.next(node, 1.0);

        List<Map<String, Object>> expected = Arrays.asList(
                MapUtil.map(CATEGORY_KEY, 1L, WEIGHT_KEY, 3.0),
                MapUtil.map(CATEGORY_KEY, 2L, WEIGHT_KEY, 2.0),
                MapUtil.map(CATEGORY_KEY, 3L, WEIGHT_KEY, 1.0)
        );

        assertThat(aggregator.result(), is(expected));
    }

}