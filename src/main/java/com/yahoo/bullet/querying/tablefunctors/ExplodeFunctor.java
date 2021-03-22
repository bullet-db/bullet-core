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
import com.yahoo.bullet.record.VirtualBulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExplodeFunctor extends TableFunctor {
    private static final long serialVersionUID = -6412197830718118997L;

    final Evaluator field;
    final String keyAlias;
    final String valueAlias;
    final boolean lateralView;
    final boolean outer;

    public ExplodeFunctor(Explode explode) {
        field = explode.getField().getEvaluator();
        keyAlias = explode.getKeyAlias();
        valueAlias = explode.getValueAlias();
        lateralView = explode.isLateralView();
        outer = explode.isOuter();
    }

    @Override
    public List<BulletRecord> apply(BulletRecord record, BulletRecordProvider provider) {
        if (valueAlias != null) {
            return explodeMap(record, provider);
        } else {
            return explodeList(record, provider);
        }
    }

    private TypedObject getField(BulletRecord record) {
        try {
            return field.evaluate(record);
        } catch (Exception ignored) {
            return TypedObject.NULL;
        }
    }

    private List<BulletRecord> explodeMap(BulletRecord record, BulletRecordProvider provider) {
        TypedObject typedObject = getField(record);
        if (!typedObject.isMap() || typedObject.size() == 0) {
            return emptyExplode(record, provider);
        }
        Map<String, Serializable> map = (Map<String, Serializable>) typedObject.getValue();
        return map.entrySet().stream().map(entry -> getRecord(entry, record, provider)).collect(Collectors.toList());
    }

    private List<BulletRecord> explodeList(BulletRecord record, BulletRecordProvider provider) {
        TypedObject typedObject = getField(record);
        if (!typedObject.isList() || typedObject.size() == 0) {
            return emptyExplode(record, provider);
        }
        List<Serializable> list = (List<Serializable>) typedObject.getValue();
        return list.stream().map(object -> getRecord(object, record, provider)).collect(Collectors.toList());
    }

    private List<BulletRecord> emptyExplode(BulletRecord record, BulletRecordProvider provider) {
        if (!outer) {
            return Collections.emptyList();
        }
        if (lateralView) {
            return Collections.singletonList(record);
        } else {
            return Collections.singletonList(provider.getInstance());
        }
    }

    private BulletRecord getRecord(Map.Entry<String, Serializable> entry, BulletRecord record, BulletRecordProvider provider) {
        BulletRecord generated = provider.getInstance();
        if (entry.getKey() != null) {
            generated.typedSet(keyAlias, new TypedObject(entry.getKey()));
        }
        if (entry.getValue() != null) {
            generated.typedSet(valueAlias, new TypedObject(entry.getValue()));
        }
        if (lateralView) {
            return new VirtualBulletRecord(record, generated);
        }
        return generated;
    }

    private BulletRecord getRecord(Serializable object, BulletRecord record, BulletRecordProvider provider) {
        BulletRecord generated = provider.getInstance();
        if (object != null) {
            generated.typedSet(keyAlias, new TypedObject(object));
        }
        if (lateralView) {
            return new VirtualBulletRecord(record, generated);
        }
        return generated;
    }
}
