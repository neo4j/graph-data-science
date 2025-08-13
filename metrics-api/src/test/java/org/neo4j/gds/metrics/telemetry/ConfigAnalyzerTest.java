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
package org.neo4j.gds.metrics.telemetry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.logging.GdsTestLog;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigAnalyzerTest {

    CypherMapWrapper defaultConfig;
    GdsTestLog log;

    @BeforeEach
    void setUp() {
        this.defaultConfig = CypherMapWrapper.create(Map.of("abstractMethod", "foo"));
        this.log = new GdsTestLog();
    }

    @Test
    void shouldReturnEmptyListForConfigWithAllDefaultValues() {
        var config = new TestConfigImpl(defaultConfig);

        var result = ConfigAnalyzer.nonDefaultParameters(config, log);

        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnMethodNamesForOptionalParametersWhenSet() {
        var config = new TestConfigImpl(defaultConfig.withEntry("optionalParam", "some-value"));

        var result = ConfigAnalyzer.nonDefaultParameters(config, log);
        assertThat(result).containsExactly("optionalParam");
    }

    @Test
    void shouldIgnoreGeneratedMethods() {
        var config = new TestConfigImpl(defaultConfig);

        var result = ConfigAnalyzer.nonDefaultParameters(config, log);
        assertThat(result).doesNotContain("ignoredMethod", "collectKeysMethod", "toMap");
    }

    @Test
    void shouldIgnoreMethodsWithParameters() {
        var config = new TestConfigImpl(defaultConfig);
        var result = ConfigAnalyzer.nonDefaultParameters(config, log);

        assertThat(result).doesNotContain("methodWithParameters");
    }

    @Test
    void shouldReturnMethodNamesForChangedDefaultImplementations() {
        var config = new TestConfigImpl(defaultConfig.withEntry("defaultMethodWithOverride", "non-default-value"));

        var result = ConfigAnalyzer.nonDefaultParameters(config, log);
        assertThat(result).contains("defaultMethodWithOverride");
    }


    interface BaseTestConfig extends AlgoBaseConfig {
        @Configuration.Key("optionalParam")
        Optional<String> optionalParam();

        @Configuration.Key("defaultMethodWithOverride")
        default String defaultMethodWithOverride() {
            return "default-value";
        }

        @Configuration.Ignore
        default String ignoredMethod() {
            return "ignored";
        }

        @Configuration.CollectKeys
        default Collection<String> collectKeysMethod() {
            return List.of("key1", "key2");
        }

        @Configuration.ToMap
        default Map<String, Object> toMapMethod() {
            return Map.of();
        }

        @Configuration.Ignore
        default String methodWithParameters(String param) {
            return "param";
        }

        // Abstract method without default implementation
        String abstractMethod();
    }

    @Configuration
    interface TestConfig extends BaseTestConfig {}
}