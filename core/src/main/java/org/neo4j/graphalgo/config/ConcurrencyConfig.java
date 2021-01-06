/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.config;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.core.GdsEdition;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface ConcurrencyConfig {

    String CONCURRENCY_KEY = "concurrency";
    int DEFAULT_CONCURRENCY = 4;
    int CONCURRENCY_LIMITATION = 4;

    @Value.Default
    @Configuration.Key(CONCURRENCY_KEY)
    default int concurrency() {
        return DEFAULT_CONCURRENCY;
    }

    @Value.Check
    default void validateConcurrency() {
        validateConcurrency(concurrency(), CONCURRENCY_KEY);
    }

    static void validateConcurrency(int requestedConcurrency, String configKey) {
        if (GdsEdition.instance().isOnCommunityEdition() && requestedConcurrency > CONCURRENCY_LIMITATION) {
            throw new IllegalArgumentException(formatWithLocale(
                "The configured `%1$s` value is too high. " +
                "The maximum allowed `%1$s` value is %2$d but %3$d was configured. " +
                "Please see the documentation (System Requirements section) for an explanation of concurrency limitations for different editions of Neo4j Graph Data Science. " +
                "Higher than concurrency %2$d is only available, when you have licensed the Enterprise Edition of the Neo4j Graph Data Science Library.",
                configKey,
                CONCURRENCY_LIMITATION,
                requestedConcurrency
            ));
        }
    }
}
