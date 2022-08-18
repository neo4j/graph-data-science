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
package org.neo4j.gds.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Just something robust around the Neo4j integration, default values, field restrictions, yada yada
 */
class DefaultsConfigurationProcedureIntegrationTest extends BaseProcTest {
    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            DefaultsConfigurationProcedure.class
        );
    }

    /**
     * The effort to build out administrator access for the set procedure is not worth the effort.
     *
     * All this test does it very assumptions about Neo4j's procedure framework and our declarations around optional
     * parameters. This is all illustrated well enough in the below test case.
     */
    @Disabled
    @Test
    void shouldSetAndListGlobalDefaults() {
        fail("Not implemented");
    }

    @Test
    void shouldSetAndListPersonalDefaults() {
        runQuery("Jonas Vingegaard", "CALL gds.alpha.config.defaults.set('concurrency', 3, 'Jonas Vingegaard')");
        runQuery("Jonas Vingegaard", "CALL gds.alpha.config.defaults.set('parallelism', 7, 'Jonas Vingegaard')");

        runQueryWithResultConsumer(
            "Jonas Vingegaard",
            "CALL gds.alpha.config.defaults.list({ username: 'Jonas Vingegaard' })",
            r -> {
                Map<String, Object> row = r.next();
                assertThat(row.get("key")).isEqualTo("concurrency");
                assertThat(row.get("value")).isEqualTo(3L);
                row = r.next();
                assertThat(row.get("key")).isEqualTo("parallelism");
                assertThat(row.get("value")).isEqualTo(7L);
                assertThat(r.hasNext()).isFalse();
            }
        );
    }

    @Test
    void shouldListPersonalDefaultsByKey() {
        runQuery("Jonas Vingegaard", "CALL gds.alpha.config.defaults.set('concurrency', 3, 'Jonas Vingegaard')");
        runQuery("Jonas Vingegaard", "CALL gds.alpha.config.defaults.set('parallelism', 7, 'Jonas Vingegaard')");

        runQueryWithResultConsumer(
            "Jonas Vingegaard",
            "CALL gds.alpha.config.defaults.list({ username: 'Jonas Vingegaard', key: 'concurrency' })",
            r -> {
                Map<String, Object> row = r.next();
                assertThat(row.get("key")).isEqualTo("concurrency");
                assertThat(row.get("value")).isEqualTo(3L);
                assertThat(r.hasNext()).isFalse();
            }
        );
    }
}
