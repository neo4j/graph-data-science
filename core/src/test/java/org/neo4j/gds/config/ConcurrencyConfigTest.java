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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Concurrency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConcurrencyConfigTest {

    @Test
    void lowConcurrency() {
        var concurrencyConfig = CypherMapWrapper.empty().withNumber("concurrency", 2);
        var algoConfig = TestConcurrencyConfig.of(concurrencyConfig);
        assertThat(algoConfig.concurrency()).isEqualTo(new Concurrency(2));
    }

    @Test
    void lowWriteConcurrency() {
        var concurrencyConfig = CypherMapWrapper.empty().withNumber("writeConcurrency", 2);
        var algoConfig = TestConcurrencyConfig.of(concurrencyConfig);
        assertThat(algoConfig.writeConcurrency()).isEqualTo(new Concurrency(2));
    }

    @Test
    void tooHighConcurrency() {
        var concurrencyConfig = CypherMapWrapper.empty().withNumber("concurrency", 1337);
        assertThatThrownBy(() -> TestConcurrencyConfig.of(concurrencyConfig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("concurrency")
            .hasMessageContaining("1337");
    }

    @Test
    void tooHighWriteConcurrency() {
        var concurrencyConfig = CypherMapWrapper.empty().withNumber("writeConcurrency", 1337);
        assertThatThrownBy(() -> TestConcurrencyConfig.of(concurrencyConfig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("writeConcurrency")
            .hasMessageContaining("1337");
    }

    @Configuration
    interface TestConcurrencyConfig extends ConcurrencyConfig, WriteConfig {
        static TestConcurrencyConfig of(CypherMapWrapper map) {
            return new TestConcurrencyConfigImpl(map);
        }
    }
}
