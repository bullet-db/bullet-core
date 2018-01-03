package com.yahoo.bullet.common;

import java.util.Arrays;

public interface Closable {
    /**
     * Returns true if this is currently closed.
     *
     * @return A boolean denoting whether this object is current closed.
     */
    boolean isClosed();

    /**
     * Returns true if any of the provided {@link Closable} objects are closed.
     *
     * @param objects A non-null array of non-null closable objects.
     * @return A boolean denoting whether any of the objects were closed.gT
     */
    static boolean isClosed(Closable... objects) {
        return Arrays.stream(objects).anyMatch(c -> c.isClosed());
    }
}
