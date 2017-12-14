package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Getter @Setter @Slf4j
public class Window {
    @Expose
    private Map<String, Object> emit;
    @Expose
    private Map<String, Object> include;

    /**
     * Default constructor. GSON recommended.
     */
    public Window() {
        emit = null;
        include = null;
    }
}
