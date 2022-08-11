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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class LimitsConfigurationTest {
    @Test
    void shouldHaveNoLimitsByDefault() {
        LimitsConfiguration limits = new LimitsConfiguration(Collections.emptyMap(), Collections.emptyMap());

        Set<LimitViolation> violations = limits.validate(Map.of("concurrency", 42), "Jonas Vingegaard");

        assertThat(violations).isEmpty();
    }

    @Test
    void shouldFindGlobalLimitViolations() {
        LimitsConfiguration limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(23)),
            Collections.emptyMap()
        );

        Set<LimitViolation> violations = limits.validate(Map.of("concurrency", 42), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("23"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldFindPersonalLimitViolations() {
        LimitsConfiguration limits = new LimitsConfiguration(
            Collections.emptyMap(),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(23)))
        );

        Set<LimitViolation> violations = limits.validate(Map.of("concurrency", 42), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("23"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldReportPersonalLimitViolationsOverGlobalOnes() {
        LimitsConfiguration limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(23)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(17)))
        );

        Set<LimitViolation> violations = limits.validate(Map.of("concurrency", 42), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("17"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldReportPersonalLimitViolationsAsTheyAreMoreSpecific() {
        LimitsConfiguration limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(17)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(23)))
        );

        Set<LimitViolation> violations = limits.validate(Map.of("concurrency", 42), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("23"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldReportGlobalLimitViolationsWhenPersonalLimitsAreNotViolated() {
        LimitsConfiguration limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(17)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(87)))
        );

        Set<LimitViolation> violations = limits.validate(Map.of("concurrency", 42), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("17"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldIgnoreLimitsThatAreNotViolated() {
        LimitsConfiguration limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(42)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(87)))
        );

        Set<LimitViolation> violations = limits.validate(Map.of("concurrency", 23), "Jonas Vingegaard");

        assertThat(violations).isEmpty();
    }

    @Test
    void shouldIgnoreLimitsThatDoNotApply() {
        LimitsConfiguration limits = new LimitsConfiguration(
            Map.of("cleverness", new Limit(23)),
            Map.of("Jonas Vingegaard", Map.of("cleverness", new Limit(42)))
        );

        Set<LimitViolation> violations = limits.validate(Map.of("concurrency", 87), "Jonas Vingegaard");

        assertThat(violations).isEmpty();
    }
}
