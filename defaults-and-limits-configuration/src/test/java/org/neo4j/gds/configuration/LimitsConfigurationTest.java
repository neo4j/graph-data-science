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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LimitsConfigurationTest {
    @Test
    void shouldHaveNoLimitsByDefault() {
        var limits = new LimitsConfiguration(Collections.emptyMap(), Collections.emptyMap());

        var violations = limits.validate(Map.of("concurrency", 42), "Jonas Vingegaard");

        assertThat(violations).isEmpty();
    }

    @Test
    void shouldFindGlobalLimitViolations() {
        var limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(23)),
            Collections.emptyMap()
        );

        var violations = limits.validate(Map.of("concurrency", 42L), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("23"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldFindPersonalLimitViolations() {
        var limits = new LimitsConfiguration(
            Collections.emptyMap(),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(23)))
        );

        var violations = limits.validate(Map.of("concurrency", 42L), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("23"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldReportPersonalLimitViolationsOverGlobalOnes() {
        var limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(23)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(17)))
        );

        var violations = limits.validate(Map.of("concurrency", 42L), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("17"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldReportPersonalLimitViolationsAsTheyAreMoreSpecific() {
        var limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(17)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(23)))
        );

        var violations = limits.validate(Map.of("concurrency", 42L), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("23"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldReportGlobalLimitViolationsWhenPersonalLimitsAreNotViolated() {
        var limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(17)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(87)))
        );

        var violations = limits.validate(Map.of("concurrency", 42L), "Jonas Vingegaard");

        assertThat(violations.size()).isEqualTo(1);
        assertThat(violations.iterator().next())
            .satisfies(lv -> assertThat(lv.getKey()).isEqualTo("concurrency"))
            .satisfies(lv -> assertThat(lv.getLimitValue()).isEqualTo("17"))
            .satisfies(lv -> assertThat(lv.getProvidedValue()).isEqualTo("42"));
    }

    @Test
    void shouldIgnoreLimitsThatAreNotViolated() {
        var limits = new LimitsConfiguration(
            Map.of("concurrency", new Limit(42)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Limit(87)))
        );

        var violations = limits.validate(Map.of("concurrency", 23L), "Jonas Vingegaard");

        assertThat(violations).isEmpty();
    }

    @Test
    void shouldIgnoreLimitsThatDoNotApply() {
        var limits = new LimitsConfiguration(
            Map.of("cleverness", new Limit(23)),
            Map.of("Jonas Vingegaard", Map.of("cleverness", new Limit(42)))
        );

        var violations = limits.validate(Map.of("concurrency", 87L), "Jonas Vingegaard");

        assertThat(violations).isEmpty();
    }

    @Test
    void shouldListGlobalLimits() {
        var configuration = new LimitsConfiguration(
            Map.of("foo", new Limit(42), "bar", new Limit(87)),
            null
        );

        var limits = configuration.list(Optional.empty(), Optional.empty());

        assertThat(limits).isEqualTo(Map.of("foo", 42L, "bar", 87L));
    }

    @Test
    void shouldListGlobalLimitsByKey() {
        var configuration = new LimitsConfiguration(
            Map.of("foo", new Limit(42), "bar", new Limit(87)),
            null
        );

        var limits = configuration.list(Optional.empty(), Optional.of("foo"));

        assertThat(limits).isEqualTo(Collections.singletonMap("foo", 42L));
    }

    @Test
    void shouldListPersonalLimits() {
        var configuration = new LimitsConfiguration(
            Collections.emptyMap(),
            Map.of("Jonas Vingegaard", Map.of("foo", new Limit(42), "bar", new Limit(87)))
        );

        var limits = configuration.list(Optional.of("Jonas Vingegaard"), Optional.empty());

        assertThat(limits).isEqualTo(Map.of("foo", 42L, "bar", 87L));
    }

    @Test
    void shouldListPersonalLimitsByKey() {
        var configuration = new LimitsConfiguration(
            Collections.emptyMap(),
            Map.of("Jonas Vingegaard", Map.of("foo", new Limit(42), "bar", new Limit(87)))
        );

        var limits = configuration.list(Optional.of("Jonas Vingegaard"), Optional.of("bar"));

        assertThat(limits).isEqualTo(Map.of("bar", 87L));
    }

    @Test
    void shouldListEffectiveLimits() {
        var configuration = new LimitsConfiguration(
            Map.of("foo", new Limit(42), "bar", new Limit(87)),
            Map.of("Jonas Vingegaard", Map.of("foo", new Limit(23), "baz", new Limit(117)))
        );

        var limits = configuration.list(Optional.of("Jonas Vingegaard"), Optional.empty());

        assertThat(limits).isEqualTo(Map.of("foo", 23L, "bar", 87L, "baz", 117L));
    }

    @Test
    void shouldListEffectiveLimitsByKey() {
        var configuration = new LimitsConfiguration(
            Map.of("foo", new Limit(42), "bar", new Limit(87)),
            Map.of("Jonas Vingegaard", Map.of("foo", new Limit(23), "baz", new Limit(117)))
        );

        var limits = configuration.list(Optional.of("Jonas Vingegaard"), Optional.of("foo"));

        assertThat(limits).isEqualTo(Map.of("foo", 23L));
    }

    @Test
    void shouldSetGlobalLimit() {
        var configuration = new LimitsConfiguration(new HashMap<>(), null);

        configuration.set("foo", 42L, Optional.empty());

        var listing = configuration.list(Optional.empty(), Optional.of("foo"));

        assertThat(listing.get("foo")).isEqualTo(42L);
    }

    @Test
    void shouldOverwriteGlobalLimit() {
        var configuration = new LimitsConfiguration(new HashMap<>(), null);

        configuration.set("foo", 42L, Optional.empty());
        configuration.set("foo", 87L, Optional.empty());

        var listing = configuration.list(Optional.empty(), Optional.of("foo"));

        assertThat(listing.get("foo")).isEqualTo(87L);
    }

    @Test
    void shouldSetPersonalLimit() {
        var configuration = new LimitsConfiguration(Collections.emptyMap(), new HashMap<>());

        configuration.set("foo", 42L, Optional.of("Jonas Vingegaard"));

        var listing = configuration.list(Optional.of("Jonas Vingegaard"), Optional.of("foo"));

        assertThat(listing.get("foo")).isEqualTo(42L);
    }

    @Test
    void shouldOverwritePersonalLimit() {
        var configuration = new LimitsConfiguration(Collections.emptyMap(), new HashMap<>());

        configuration.set("foo", 42L, Optional.of("Jonas Vingegaard"));
        configuration.set("foo", 87L, Optional.of("Jonas Vingegaard"));

        var listing = configuration.list(Optional.of("Jonas Vingegaard"), Optional.of("foo"));

        assertThat(listing.get("foo")).isEqualTo(87L);
    }
}
