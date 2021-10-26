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
package org.neo4j.gds.config;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.concurrency.ConcurrencyValidatorService;
import org.neo4j.gds.core.concurrency.ParallelUtil;

public interface ConcurrencyConfig {

    String CONCURRENCY_KEY = "concurrency";
    int DEFAULT_CONCURRENCY = 4;
    int CONCURRENCY_LIMITATION = 4;

    @Value.Default
    @Configuration.Key(CONCURRENCY_KEY)
    default int concurrency() {
        return DEFAULT_CONCURRENCY;
    }

    @Value.Default
    @Configuration.Ignore
    default int minBatchSize() {
        return ParallelUtil.DEFAULT_BATCH_SIZE;
    }

    @Value.Check
    default void validateConcurrency() {
        ConcurrencyValidatorService.validator().validate(concurrency(), CONCURRENCY_KEY, CONCURRENCY_LIMITATION);
    }

}
