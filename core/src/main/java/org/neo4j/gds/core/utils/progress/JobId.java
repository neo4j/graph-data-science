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
package org.neo4j.gds.core.utils.progress;

import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

public final class JobId {
    private final String value;

    public JobId() {
        this.value = UUID.randomUUID().toString();
    }

    public JobId(UUID id) {
        this(id.toString());
    }

    public JobId(String id) {
        this.value = id;
    }

    public String asString() {
        return value;
    }

    @Override
    public String toString() {
        return "JobId(" + value + ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobId that = (JobId) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    public static JobId parse(Object input) {
        if (input instanceof String) {
            return new JobId((String) input);
        } else if (input instanceof JobId) {
            return (JobId) input;
        }

        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "Expected JobId or String. Got %s.",
            input.getClass().getSimpleName()
        ));
    }

    public static String asString(JobId jobId) {
        return jobId.asString();
    }
}
