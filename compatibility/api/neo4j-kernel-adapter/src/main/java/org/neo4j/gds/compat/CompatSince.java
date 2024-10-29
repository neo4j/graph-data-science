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
package org.neo4j.gds.compat;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker interface to help with keeping the compat layer as small as necessary.
 * <p>
 * Annotating a method in `Neo4jProxyApi` with `@CompatSince(minor=42)` would mean that that method
 * needed to be added to the compat layer because of a change in Neo4j 5.42.
 * Consequently, when we drop support for a version such that the oldest supported Neo4j version is 5.42,
 * we know that we can remove all compat method with a value of `minor=42` (or lower).
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface CompatSince {

    /**
     * The Neo4j major version that brought the change that required
     * this compatibility method to exist.
     */
    int major() default 5;

    /**
     * The Neo4j minor version that brought the change that required
     * this compatibility method to exist.
     */
    int minor();
}
