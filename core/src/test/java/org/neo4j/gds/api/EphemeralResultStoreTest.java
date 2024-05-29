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
package org.neo4j.gds.api;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.extension.FakeClockExtension;
import org.neo4j.gds.extension.Inject;
import org.neo4j.time.FakeClock;

import java.util.List;
import java.util.function.LongUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@FakeClockExtension
class EphemeralResultStoreTest {

    @Inject
    FakeClock clock;

    private static final JobId JOB_ID = new JobId("test");

    @Test
    void shouldStoreEntry() {
        var resultStore = new EphemeralResultStore();

        var propertyValues = mock(NodePropertyValues.class);
        var toOriginalId = mock(LongUnaryOperator.class);
        var nodePropertiesEntry = new ResultStoreEntry.NodeProperties(
            List.of("A", "B"),
            List.of("foo"),
            List.of(propertyValues),
            toOriginalId
        );
        resultStore.add(JOB_ID, nodePropertiesEntry);

        assertThat(resultStore.hasEntry(JOB_ID)).isTrue();
        assertThat(resultStore.get(JOB_ID)).isEqualTo(nodePropertiesEntry);
    }

    @Test
    void shouldNotResolveEntryWhenJobIdDoesNotMatch() {
        var resultStore = new EphemeralResultStore();

        var propertyValues = mock(NodePropertyValues.class);
        var toOriginalId = mock(LongUnaryOperator.class);
        var nodePropertiesEntry = new ResultStoreEntry.NodeProperties(
            List.of("A", "B"),
            List.of("foo"),
            List.of(propertyValues),
            toOriginalId
        );
        resultStore.add(JOB_ID, nodePropertiesEntry);

        assertThat(resultStore.hasEntry(new JobId("foo"))).isFalse();
        assertThat(resultStore.get(new JobId("foo"))).isNull();
    }

    @Test
    void shouldRemoveEntry() {
        var resultStore = new EphemeralResultStore();

        var propertyValues = mock(NodePropertyValues.class);
        var toOriginalId = mock(LongUnaryOperator.class);
        var nodePropertiesEntry = new ResultStoreEntry.NodeProperties(
            List.of("A", "B"),
            List.of("foo"),
            List.of(propertyValues),
            toOriginalId
        );
        resultStore.add(JOB_ID, nodePropertiesEntry);

        assertThat(resultStore.hasEntry(JOB_ID)).isTrue();

        resultStore.remove(JOB_ID);

        assertThat(resultStore.hasEntry(JOB_ID)).isFalse();
        assertThat(resultStore.get(JOB_ID)).isNull();
    }

    @Test
    void shouldEvictEntryAfter10Minutes() throws InterruptedException {
        var resultStore = new EphemeralResultStore();

        var propertyValues = mock(NodePropertyValues.class);
        var toOriginalId = mock(LongUnaryOperator.class);
        var nodePropertiesEntry = new ResultStoreEntry.NodeProperties(
            List.of("A", "B"),
            List.of("foo"),
            List.of(propertyValues),
            toOriginalId
        );
        resultStore.add(JOB_ID, nodePropertiesEntry);

        assertThat(resultStore.hasEntry(JOB_ID)).isTrue();

        clock.forward(EphemeralResultStore.CACHE_EVICTION_DURATION.plusMinutes(1));
        // make some room for the cache eviction thread to trigger a cleanup
        Thread.sleep(100);

        assertThat(resultStore.hasEntry(JOB_ID)).isFalse();
    }
}
