/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;
import static com.yahoo.bullet.common.Utilities.extractTypedObject;
import static com.yahoo.bullet.common.Utilities.getCasted;

@Getter
public class OrderBy implements PostStrategy {
    public enum Direction {
        ASC,
        DESC,
        UNKNOWN;

        /**
         * Constructs a {@link Direction} from a string.
         *
         * @param s The string to be constructed from.
         * @return The constructed {@link Direction}.
         */
        public static Direction of(String s) {
            switch (s.toUpperCase()) {
                case "ASC":
                    return ASC;
                case "DESC":
                    return DESC;
                default:
                    return UNKNOWN;
            }
        }
    }

    private static final String FIELDS_NAME = "fields";
    private static final String DIRECTION_NAME = "direction";
    public static final BulletError ORDERBY_REQUIRES_FIELDS_ERROR =
            makeError("The ORDERBY post aggregation needs at least one field", "Please add fields.");
    public static final BulletError ORDERBY_UNKNOWN_DIRECTION_ERROR =
            makeError("The direction of ORDERBY post aggregation is not recognized", "Please provide direction as ASC | DESC.");

    private List<String> fields;
    private Direction direction = Direction.ASC;

    /**
     * Constructor takes a {@link PostAggregation}.
     *
     * @param aggregation An {@link PostAggregation} for this post aggregation type.
     */
    @SuppressWarnings("unchecked")
    public OrderBy(PostAggregation aggregation) {
        Map<String, Object> attributes = aggregation.getAttributes();
        if (attributes != null) {
            fields = getCasted(attributes, FIELDS_NAME, List.class);
            String d = getCasted(attributes, DIRECTION_NAME, String.class);
            if (d != null) {
                direction = Direction.of(d);
            }
        }
    }

    @Override
    public Clip execute(Clip clip) {
        List<BulletRecord> records = clip.getRecords();
        records.sort((a, b) -> {
                for (String field : fields) {
                    TypedObject typedObjectA = extractTypedObject(field, a);
                    TypedObject typedObjectB = extractTypedObject(field, b);
                    try {
                        int compareValue = typedObjectA.compareTo(typedObjectB);
                        if (compareValue != 0) {
                            return direction == Direction.ASC ? compareValue : -1 * compareValue;
                        }
                    } catch (RuntimeException e) {
                        // Ignore the exception and skip this field.
                    }
                }
                return 0;
            });
        return clip;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        if (fields == null || fields.isEmpty()) {
            return Optional.of(Collections.singletonList(ORDERBY_REQUIRES_FIELDS_ERROR));
        }
        if (direction == Direction.UNKNOWN) {
            return Optional.of(Collections.singletonList(ORDERBY_UNKNOWN_DIRECTION_ERROR));
        }
        return Optional.empty();
    }
}
