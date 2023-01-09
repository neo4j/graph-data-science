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
package org.neo4j.gds.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can be used to annotate a procedure in the same way as using `@Procedure` would.
 * However, this annotation does not actually register anything with the procedure framework.
 * We can use it to provide as quasi-equivalent method for out documentation and test tooling
 * for procedures and functions that cannot use actual procedure framework annotations.
 * It can also be used to pretend that `@UserAggregationFunction`s have a `@Procedure` annotation.
 * <p>
 * The return type of the annotated method should be a proper Java interface, not a Map,
 * so that we can extract the schema from the fields.
 * Only fields marked with the nested `@CustomProcedure.ResultField` annotation are included in the schema.
 * <p>
 * The parameters of the annotated method should be annotated with `@Name` as you would for regular `@Procedure` methods.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CustomProcedure {

    /**
     * The name of the procedure.
     */
    String value();

    /**
     * Annotated methods are included in the result definition.
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface ResultField {
    }
}
