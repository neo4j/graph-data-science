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
package org.neo4j.gds.extension;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.core.Aggregation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

@Target(FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(GdlGraphs.class)
public @interface GdlGraph {

    String graphNamePrefix() default "";

    Orientation orientation() default Orientation.NATURAL;

    Aggregation aggregation() default Aggregation.NONE;

    PropertyState propertyState() default PropertyState.TRANSIENT;

    /**
     * Set to true, if the relationships in the graph should be inverse indexed.
     */
    boolean indexInverse() default false;

    String username() default "";

    /**
     * Offset for assigning ids to nodes in the GDL graph.
     * Node ids will always be consecutive incrementing by 1.
     */
    long idOffset() default 0;

    /**
     * If set, the graph store is added to the GraphStore catalog.
     * The name is the {@code graphNamePrefix() + 'Graph'} or just
     * {@code 'graph'} if no prefix is set.
     */
    boolean addToCatalog() default false;
}
