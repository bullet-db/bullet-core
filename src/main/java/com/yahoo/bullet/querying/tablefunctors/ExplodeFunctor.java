/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.tablefunctors;

import com.yahoo.bullet.query.tablefunctions.Explode;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A table functor that flattens the result of the given evaluator into a list of Bullet records. If only the key alias
 * is specified, the result is assumed to be a list, and each element of the list is inserted into its own record as a
 * field named by the key alias. If the value alias is also specified, the result is instead assumed to be a map,
 * and each key-value pair of the map is inserted into its own record as two fields named respectively by the key and
 * value aliases. If the result of the evaluator has the wrong type or is null, the table functor returns an empty list.
 */
public class ExplodeFunctor extends OuterableTableFunctor {
    private static final long serialVersionUID = -6412197830718118997L;

    final Evaluator field;
    final String keyAlias;
    final String valueAlias;

    /**
     * Constructor that creates an explode table functor from a {@link Explode}.
     *
     * @param explode The explode table function to construct the table functor from.
     */
    public ExplodeFunctor(Explode explode) {
        super(explode.isOuter());
        field = explode.getField().getEvaluator();
        keyAlias = explode.getKeyAlias();
        valueAlias = explode.getValueAlias();
    }

    @Override
    protected List<BulletRecord> outerableApply(BulletRecord record, BulletRecordProvider provider) {
        if (valueAlias != null) {
            return explodeMap(record, provider);
        } else {
            return explodeList(record, provider);
        }
    }

    private List<BulletRecord> explodeMap(BulletRecord record, BulletRecordProvider provider) {
        TypedObject typedObject = getField(record);
        if (!typedObject.isMap() || typedObject.size() == 0) {
            return Collections.emptyList();
        }
        Map<String, Serializable> map = (Map<String, Serializable>) typedObject.getValue();
        return map.entrySet().stream().map(entry -> getRecord(entry, provider)).collect(Collectors.toList());
    }

    private List<BulletRecord> explodeList(BulletRecord record, BulletRecordProvider provider) {
        TypedObject typedObject = getField(record);
        if (!typedObject.isList() || typedObject.size() == 0) {
            return Collections.emptyList();
        }
        List<Serializable> list = (List<Serializable>) typedObject.getValue();
        return list.stream().map(object -> getRecord(object, provider)).collect(Collectors.toList());
    }

    private TypedObject getField(BulletRecord record) {
        try {
            return field.evaluate(record);
        } catch (Exception ignored) {
            return TypedObject.NULL;
        }
    }

    private BulletRecord getRecord(Map.Entry<String, Serializable> entry, BulletRecordProvider provider) {
        BulletRecord generated = provider.getInstance();
        if (entry.getKey() != null) {
            generated.typedSet(keyAlias, new TypedObject(entry.getKey()));
        }
        if (entry.getValue() != null) {
            generated.typedSet(valueAlias, new TypedObject(entry.getValue()));
        }
        return generated;
    }

    private BulletRecord getRecord(Serializable object, BulletRecordProvider provider) {
        BulletRecord generated = provider.getInstance();
        if (object != null) {
            generated.typedSet(keyAlias, new TypedObject(object));
        }
        return generated;
    }
}
