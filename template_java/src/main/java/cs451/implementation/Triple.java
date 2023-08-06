package cs451.implementation;

import java.util.Objects;



    /**
     * Container to ease passing around a tuple of two objects. This object provides a sensible
     * implementation of equals(), returning true if equals() is true on each of the contained
     * objects.
     *
     * Note: taken from one of the entries at https://stackoverflow.com/questions/5303539/didnt-java-once-have-a-pair-class
     * Slightly modified to accommodate for the project's use case
     */
    public class Triple<F, S, L> {
        public final F first;
        public final S second;
        public final L third;

        /**
         * Constructor for a Pair.
         *
         * @param first the first object in the Pair
         * @param second the second object in the pair
         */
        public Triple(F first, S second, L third) {
            this.first = first;
            this.second = second;
            this.third=third;
        }

        /**
         * Checks the two objects for equality by delegating to their respective
         * {@link Object#equals(Object)} methods.
         *
         * @param o the {@link cs451.implementation.Triple} to which this one is to be checked for equality
         * @return true if the underlying objects of the Triple are both considered
         *         equal
         */
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof cs451.implementation.Triple)) {
                return false;
            }
            cs451.implementation.Triple<?, ?,?> p = (cs451.implementation.Triple<?, ?,?>) o;
            return Objects.equals(p.first, first) && Objects.equals(p.second, second) && Objects.equals(p.third,third);
        }

        /**
         * String representation of the Triple
         * @return String representation of the Triple
         */
        @Override
        public String toString() {
            return "Triple{" +
                    "first=" + first +
                    ", second=" + second +
                    ", third=" + third +
                    '}';
        }


        /**
         * Compute a hash code using the hash codes of the underlying objects
         *
         * @return a hashcode of the Pair
         */
        @Override
        public int hashCode() {
            return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode()) ^ (third == null ? 0 : third.hashCode());
        }

        /**
         * Convenience method for creating an appropriately typed pair.
         * @param a the first object in the Pair
         * @param b the second object in the pair
         * @return a Pair that is templatized with the types of a and b
         */
        public static <A, B,C> cs451.implementation.Triple<A, B, C> create(A a, B b,C c) {
            return new cs451.implementation.Triple<A, B, C>(a, b,c);
        }
    }
