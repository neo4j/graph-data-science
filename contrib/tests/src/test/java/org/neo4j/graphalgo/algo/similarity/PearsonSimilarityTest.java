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
package org.neo4j.graphalgo.algo.similarity;

import org.junit.Test;
import org.neo4j.graphalgo.similarity.Similarities;
import org.neo4j.graphalgo.similarity.SimilarityVectorAggregator;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PearsonSimilarityTest {

    @Test
    public void identicalVectors() {
        Similarities similarities = new Similarities();

        List<Number> user1Ratings = Arrays.asList(5, 6, 7, 8, 9);
        List<Number> user2Ratings = Arrays.asList(4, 5, 6, 7, 8);
        double similarity = similarities.pearsonSimilarity(user1Ratings, user2Ratings, Collections.EMPTY_MAP);
        assertEquals(1.0, similarity, 0.01);
    }

    @Test
    public void oppositeVectors() {
        Similarities similarities = new Similarities();

        List<Number> user1Ratings = Arrays.asList(9, 8, 7, 6, 5);
        List<Number> user2Ratings = Arrays.asList(4, 5, 6, 7, 8);
        double similarity = similarities.pearsonSimilarity(user1Ratings, user2Ratings, Collections.EMPTY_MAP);
        assertEquals(-1.0, similarity, 0.01);
    }

    @Test
    public void identicalMaps() {
        Similarities similarities = new Similarities();

        SimilarityVectorAggregator aggregator = new SimilarityVectorAggregator();
        Node node = mock(Node.class);
        when(node.getId()).thenReturn(1L, 2L, 3L);

        aggregator.next(node, 3.0);
        aggregator.next(node, 2.0);
        aggregator.next(node, 1.0);

        List<Map<String, Object>> vectorMap = aggregator.result();

        double similarity = similarities.pearsonSimilarity(vectorMap, vectorMap, MapUtil.map("vectorType", "maps"));
        assertEquals(1.0, similarity, 0.01);
    }

    @Test
    public void oppositeMaps() {
        Similarities similarities = new Similarities();

        SimilarityVectorAggregator aggregator1 = new SimilarityVectorAggregator();
        Node node1 = mock(Node.class);
        when(node1.getId()).thenReturn(1L, 2L, 3L);
        aggregator1.next(node1, 1.0);
        aggregator1.next(node1, 2.0);
        aggregator1.next(node1, 3.0);
        List<Map<String, Object>> v1Map = aggregator1.result();

        SimilarityVectorAggregator aggregator2 = new SimilarityVectorAggregator();
        Node node2 = mock(Node.class);
        when(node2.getId()).thenReturn(1L, 2L, 3L);
        aggregator2.next(node2, 3.0);
        aggregator2.next(node2, 2.0);
        aggregator2.next(node2, 1.0);
        List<Map<String, Object>> v2Map = aggregator2.result();

        double similarity = similarities.pearsonSimilarity(v1Map, v2Map, MapUtil.map("vectorType", "maps"));
        assertEquals(-1.0, similarity, 0.01);
    }

    @Test
    public void someItemsDifferentMap() {
        Similarities similarities = new Similarities();

        SimilarityVectorAggregator aggregator1 = new SimilarityVectorAggregator();
        Node node1 = mock(Node.class);
        when(node1.getId()).thenReturn(1L, 2L, 3L, 5L);
        aggregator1.next(node1, 1.0);
        aggregator1.next(node1, 2.0);
        aggregator1.next(node1, 3.0);
        aggregator1.next(node1, 4.0);
        List<Map<String, Object>> v1Map = aggregator1.result();

        SimilarityVectorAggregator aggregator2 = new SimilarityVectorAggregator();
        Node node2 = mock(Node.class);
        when(node2.getId()).thenReturn(2L, 3L, 4L, 5L);
        aggregator2.next(node2, 2.0);
        aggregator2.next(node2, 3.0);
        aggregator2.next(node2, 1.0);
        aggregator2.next(node2, 4.0);
        List<Map<String, Object>> v2Map = aggregator2.result();


        double similarity = similarities.pearsonSimilarity(v1Map, v2Map, MapUtil.map("vectorType", "maps"));
        assertEquals(1.0, similarity, 0.01);
    }

    @Test
    public void noOverlapMap() {
        Similarities similarities = new Similarities();

        SimilarityVectorAggregator aggregator1 = new SimilarityVectorAggregator();
        Node node1 = mock(Node.class);
        when(node1.getId()).thenReturn(1L, 2L, 3L, 5L);
        aggregator1.next(node1, 1.0);
        aggregator1.next(node1, 2.0);
        aggregator1.next(node1, 3.0);
        aggregator1.next(node1, 4.0);
        List<Map<String, Object>> v1Map = aggregator1.result();

        SimilarityVectorAggregator aggregator2 = new SimilarityVectorAggregator();
        Node node2 = mock(Node.class);
        when(node2.getId()).thenReturn(6L, 7L, 8L, 9L);
        aggregator2.next(node2, 2.0);
        aggregator2.next(node2, 3.0);
        aggregator2.next(node2, 1.0);
        aggregator2.next(node2, 4.0);
        List<Map<String, Object>> v2Map = aggregator2.result();

        double similarity = similarities.pearsonSimilarity(v1Map, v2Map, MapUtil.map("vectorType", "maps"));
        assertEquals(0.0, similarity, 0.01);
    }

}
