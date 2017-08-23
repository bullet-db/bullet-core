package com.yahoo.bullet.pubsub;

import lombok.Getter;
import lombok.Setter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Getter @Setter @AllArgsConstructor @NoArgsConstructor
public class Metadata implements Serializable {
    public enum Signal {
        ACKNOWLEDGE,
        COMPLETE
    }
    private Signal signal;
    private Serializable content;

    /**
     * Check if Metadata has content.
     *
     * @return true if Metadata has content.
     */
    public boolean hasContent() {
        return content != null;
    }

    /**
     * Check if Metadata has signal.
     *
     * @return true if message has {@link Metadata#signal}
     */
    public boolean hasSignal() {
        return signal != null;
    }
}
