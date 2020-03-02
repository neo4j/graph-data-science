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
package org.neo4j.graphalgo.config;

import org.immutables.value.Value;
import org.neo4j.graphalgo.core.concurrency.ConcurrencyMonitor;

public interface ConcurrencyValidation {

    int CONCURRENCY_LIMITATION = 4;

    @Value.Check
    default void validateConcurrency() {
        if (ConcurrencyMonitor.instance().isUnlimited()) {
            // do nothing
            return;
        }
        if (this instanceof WriteConfig) {
            WriteConfig wc = (WriteConfig) this;
            Validator.validate(wc.concurrency());
            Validator.validate(wc.writeConcurrency());
        } else if (this instanceof AlgoBaseConfig) {
            AlgoBaseConfig algoConfig = (AlgoBaseConfig) this;
            Validator.validate(algoConfig.concurrency());
        } else if (this instanceof GraphCreateConfig) {
            GraphCreateConfig gcc = (GraphCreateConfig) this;
            Validator.validate(gcc.readConcurrency());
        }
    }

    class Validator {
        private static void validate(int requestedConcurrency) {
            if (requestedConcurrency > CONCURRENCY_LIMITATION) {
                throw new IllegalArgumentException(String.format(
                    "The configured concurrency value is too high. " +
                    "The maximum allowed concurrency value is %d but %d was configured. " +
                    "Please see the documentation (System Requirements section) for an explanation of concurrency limitations for different editions of Neo4j Graph Data Science. " +
                    "Higher than concurrency %d is only available under the Neo4j Graph Data Science Edition license.",
                    CONCURRENCY_LIMITATION,
                    requestedConcurrency,
                    CONCURRENCY_LIMITATION
                ));
            }
        }
    }
}
