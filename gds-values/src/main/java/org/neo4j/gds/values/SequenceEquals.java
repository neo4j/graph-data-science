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
package org.neo4j.gds.values;

/**
 * Compares the elements two {@link Sequence} values hold, for the {@code equals} overloads an {@link Array}
 * and a {@link Vector} implement alike. It takes the backing arrays rather than the values themselves,
 * because a sequence stores its elements in whichever primitive type is narrowest.
 *
 * <p>Handles only {@code a[] == b[]} where {@code type(a) != type(b)}, i.e. {@code byte[] == int[]} and such.
 * Use {@code Arrays.equals()} when both sides have the same type.
 */
public final class SequenceEquals {
    private SequenceEquals() {}

    public static boolean byteAndShort(byte[] a, short[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if (a[i] != b[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    public static boolean byteAndInt(byte[] a, int[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if (a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean byteAndLong(byte[] a, long[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((long)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean byteAndFloat(byte[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((float)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean byteAndDouble(byte[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean shortAndInt(short[] a, int[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if (a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean shortAndLong(short[] a, long[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((long)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean shortAndFloat(short[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((float)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean shortAndDouble(short[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean intAndLong(int[] a, long[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((long)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean intAndFloat(int[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((float)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean intAndDouble(int[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean longAndFloat(long[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((float)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean longAndDouble(long[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean floatAndDouble(float[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        } else {
            for(int i = 0; i < a.length; ++i) {
                if ((double)a[i] != b[i]) {
                    return false;
                }
            }

            return true;
        }
    }
}
