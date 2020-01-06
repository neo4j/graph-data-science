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
package org.neo4j.graphalgo.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface Configuration {

    /**
     * Name of the generated class
     */
    String value();

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface Key {
        String value();
    }

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface ConvertWith {
        String value();
    }

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface Ignore {
    }

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface Parameter {
        boolean acceptNull() default false;
    }
}
