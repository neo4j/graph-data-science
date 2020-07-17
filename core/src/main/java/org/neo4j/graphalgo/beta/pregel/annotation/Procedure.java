/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface Procedure {

    /**
     * The namespace and name for the procedure, as a period-separated
     * string. For instance {@code myprocedures.myprocedure}.
     *
     * If this is left empty, the name defaults to the package name of
     * the class the procedure is declared in, combined with the method
     * name. Notably, the class name is omitted.
     *
     * @return the namespace and procedure name.
     */
    String value() default "";

    /**
     * Synonym for {@link #value()}
     *
     * @return the namespace and procedure name.
     */
    String name() default "";
}
