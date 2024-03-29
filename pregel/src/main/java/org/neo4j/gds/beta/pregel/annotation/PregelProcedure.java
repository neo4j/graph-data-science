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
package org.neo4j.gds.beta.pregel.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface PregelProcedure {

    /**
     * The namespace and name for the procedure, as a period-separated
     * string. For instance {@code myprocedures.myprocedure}.
     *
     * @return the namespace and procedure name.
     */
    String name();

    /**
     * The procedure modes to generate.
     *
     * @return procedure modes
     */
    GDSMode[] modes() default {GDSMode.STREAM, GDSMode.WRITE, GDSMode.MUTATE, GDSMode.STATS};

    /**
     * A description of the procedure that can be accessed via Cypher.
     *
     * @return procedure description
     */
    String description() default "";

    /**
     * A reference to a replacement if the procedure is deprecated.
     *
     * @return procedure description
     */
    String deprecatedBy() default "";
}
