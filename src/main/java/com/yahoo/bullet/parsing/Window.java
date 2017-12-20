package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter @Setter @Slf4j
public class Window implements Configurable, Initializable {
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

    @Override
    public void configure(BulletConfig configuration) {
    }

    @Override
    public Optional<List<Error>> initialize() {
        return Optional.empty();
    }
}
