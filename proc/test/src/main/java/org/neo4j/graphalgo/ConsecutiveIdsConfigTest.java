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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.ConsecutiveIdsConfig;
import org.neo4j.graphalgo.config.SeedConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

public interface ConsecutiveIdsConfigTest<ALGORITHM extends Algorithm<ALGORITHM, RESULT>,CONFIG extends ConsecutiveIdsConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    @Test
    default void shouldProduceResultWithConsecutiveIds() {
        var graphName = "loadedGraph";
        var createGraphQuery = GdsCypher.call().loadEverything().graphCreate(graphName).yields();
        runQuery(graphDb(), createGraphQuery);

        var consecutiveIdsConfig = createMinimalConfig(CypherMapWrapper
            .empty()
            .withBoolean("consecutiveIds", true))
            .toMap();


        applyOnProcedure((proc) -> {
            var computationResult = proc.compute(graphName, consecutiveIdsConfig);
            var propertyTranslator = proc.nodePropertyTranslator(computationResult);

            assertTrue(propertyTranslator instanceof PropertyTranslator.ConsecutivePropertyTranslator);

            Set<Long> consecutiveIds = LongStream
                .range(0, computationResult.graph().nodeCount())
                .map(nodeId -> (long) propertyTranslator.toDouble(computationResult.result(), nodeId))
                .boxed()
                .collect(Collectors.toSet());

            for (long i = 0; i < consecutiveIds.size(); i++) {
                assertTrue(consecutiveIds.contains(i));
            }
        });
    }

    @Test
    default void shouldFailWhenRunWithConsecutiveIdsAndSeeding() {
        var graphName = "loadedGraph";
        var createGraphQuery = GdsCypher.call().loadEverything().graphCreate(graphName).yields();
        runQuery(graphDb(), createGraphQuery);

        CypherMapWrapper consecutiveIdsConfig = createMinimalConfig(CypherMapWrapper
            .empty()
            .withBoolean("consecutiveIds", true))
            .withEntry("seedProperty", "prop");

        applyOnProcedure((proc) -> {
            var config = proc.newConfig(Optional.of(graphName), createMinimalConfig(CypherMapWrapper.empty()));
            if (config instanceof SeedConfig) {
                var ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> proc.newConfig(Optional.of(graphName), consecutiveIdsConfig)
                );
                assertTrue(ex.getMessage().contains("Seeding and the `consecutiveIds` option cannot be used at the same time"));
            }
        });
    }
}
