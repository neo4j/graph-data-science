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
     * Name of the generated class.
     *
     * If not manually set, the value is set to the
     * annotation class name with an "Impl" suffix:
     *
     * <pre>
     * &#64;Configuration
     * interface Foo { }
     *
     * &#64;Generated
     * public class FooImpl { }
     * </pre>
     *
     */
    String value() default "";

    /**
     * By default, a configuration field is resolved in the {@link org.neo4j.gds.core.CypherMapWrapper} parameter with the method name as the expected key.
     * This annotation changes the key to lookup to use {@link org.neo4j.gds.annotation.Configuration.Key#value()} instead.
     */
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
    @interface ToMapValue {
        String value();
    }

    /**
     * Used to specify which interface methods to ignore by the ConfigurationProcessor.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface Ignore {
    }

    /**
     * Configuration field is expected to be passed to the constructor as a parameter.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface Parameter {
        boolean acceptNull() default false;
    }

    /**
     * Annotated function will return the list of configuration keys
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface CollectKeys {
    }

    /**
     * Annotated function will return the map representation of the configuration
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface ToMap {
    }

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface GraphStoreValidation {
    }

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface GraphStoreValidationCheck {
    }

    /**
     * Input for the annotated configuration field storing an Integer, will be validated if it is in the given range
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface IntegerRange {
        int min() default Integer.MIN_VALUE;
        int max() default Integer.MAX_VALUE;
        boolean minInclusive() default true;
        boolean maxInclusive() default true;
    }

    /**
     * Input for the annotated configuration field storing a Long, will be validated if it is in the given range
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface LongRange {
        long min() default Long.MIN_VALUE;
        long max() default Long.MAX_VALUE;
        boolean minInclusive() default true;
        boolean maxInclusive() default true;
    }

    /**
     * Input for the annotated configuration field storing a Double, will be validated if it is in the given range
     */
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.CLASS)
    @interface DoubleRange {
        double min() default -Double.MAX_VALUE;
        double max() default Double.MAX_VALUE;
        boolean minInclusive() default true;
        boolean maxInclusive() default true;
    }
}
