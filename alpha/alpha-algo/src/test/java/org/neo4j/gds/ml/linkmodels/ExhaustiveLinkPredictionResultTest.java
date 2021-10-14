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
package org.neo4j.gds.ml.linkmodels;

import org.junit.jupiter.api.RepeatedTest;
import org.neo4j.gds.core.concurrency.Pools;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.callable;
import static org.assertj.core.api.Assertions.assertThat;

class ExhaustiveLinkPredictionResultTest {

    @RepeatedTest(100)
    void test() throws InterruptedException {
        var queue = new ExhaustiveLinkPredictionResult(10);

        Runnable lowPrio = () -> queue.add(1, 2, -1);
        Runnable highPrio = () -> queue.add(2, 3, 1);

        Pools.DEFAULT.invokeAll(List.of(callable(lowPrio), callable(highPrio)));

        var priorityResults = queue
            .stream()
            .collect(Collectors.toList());

        assertThat(priorityResults).hasSize(2).containsExactly(PredictedLink.of(2, 3, 1), PredictedLink.of(1, 2, -1));
    }

}
