/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.results;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.impl.similarity.TopKConsumer;

import java.util.Arrays;

class SimilarityResultTest {

    @Test
    void equality() {
        SimilarityResult one = new SimilarityResult(
            5,10,100,100,0,0.014577465637590655,false,false
        );

        SimilarityResult two = new SimilarityResult(5, 10, 100, 100, 0, 0.014577465637590657, false, true);

        System.out.println(one.equals(two));

    }

    @Test
    void topK() {

        TopKConsumer<SimilarityResult> consumer = new TopKConsumer<>(3, SimilarityResult.DESCENDING);

        SimilarityResult one = new SimilarityResult(
                5,10,100,100,0,0.014577465637590655,false,false
        );

        SimilarityResult two = new SimilarityResult(5, 10, 100, 100, 0, 0.014577465637590657, false, true);

        consumer.apply(one);
        consumer.apply(two);

        System.out.println("streaming...");
        consumer.stream().forEach(System.out::println);

    }

    @Test
    void binarySearch() {
        SimilarityResult one = new SimilarityResult(
                5,10,100,100,0,0.014577465637590655,false,false
        );

        SimilarityResult two = new SimilarityResult(5, 10, 100, 100, 0, 0.014577465637590657, false, true);

        SimilarityResult[] heap = {
                one, null, null
        };

        int index1 = Arrays.binarySearch(heap, 0, 2, one, SimilarityResult.DESCENDING);
        System.out.println("index = " + index1);

        int index2 = Arrays.binarySearch(heap, 0, 2, two, SimilarityResult.DESCENDING);
        System.out.println("index = " + index2);

        System.out.println(SimilarityResult.DESCENDING.compare(one, two));


    }
}