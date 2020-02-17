/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.result;

import com.yahoo.bullet.record.BulletRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.yahoo.bullet.TestHelpers.assertJSONEquals;
import static java.util.Collections.singletonMap;

@SuppressWarnings("unchecked")
public class ClipTest {
    public static final String EMPTY_RESULT = "{\"meta\": {}, \"records\": []}";

    public static String makeJSON(String results) {
        return "{\"meta\": {}, \"records\": " + results + "}";
    }

    public static String makeJSON(String meta, String records) {
        return "{\"meta\": " + meta + ", \"records\": " + records + "}";
    }

    @Test
    public void testEmptyRecord() {
        assertJSONEquals(new Clip().asJSON(), EMPTY_RESULT);
    }

    @Test
    public void testNullRecord() {
        assertJSONEquals(new Clip().add((BulletRecord) null).asJSON(), EMPTY_RESULT);
    }

    @Test
    public void testNullRecords() {
        assertJSONEquals(new Clip().add((List<BulletRecord>) null).asJSON(), EMPTY_RESULT);
    }

    @Test
    public void testNullValueInRecord() {
        BulletRecord record = new RecordBox().addMap("map_field", Pair.of("bar", true), Pair.of("foo", null)).getRecord();
        assertJSONEquals(Clip.of(record).asJSON(), makeJSON("[{'map_field':{'bar':true,'foo':null}}]"));
    }


    @Test
    public void testRecordAddition() {
        BulletRecord record = new RecordBox().add("field", "sample").addMap("map_field", Pair.of("foo", "bar"))
                                             .addListOfMaps("list_field", new HashMap<>(), singletonMap("foo", 1L))
                                             .getRecord();
        assertJSONEquals(Clip.of(record).asJSON(), makeJSON("[{'list_field':[{},{'foo':1}],'field':'sample','map_field':{'foo':'bar'}}]"));
    }

    @Test
    public void testRecordsAddition() {
        BulletRecord record = new RecordBox().add("field", "sample").addMap("map_field", Pair.of("foo", "bar"))
                                             .addListOfMaps("list_field", new HashMap<>(), singletonMap("foo", 1L))
                                             .getRecord();

        BulletRecord another = new RecordBox().add("field", "another").getRecord();

        List<BulletRecord> list = new ArrayList<>();
        list.add(another);
        list.add(record);
        assertJSONEquals(Clip.of(list).asJSON(),
                         makeJSON("[{'field':'another'}, {'list_field':[{},{'foo':1}],'field':'sample','map_field':{'foo':'bar'}}]"));
    }

    @Test
    public void testInvalidDoubles() {
        BulletRecord record = new RecordBox().addNull("field").add("plus_inf", Double.POSITIVE_INFINITY)
                                             .add("neg_inf", Double.NEGATIVE_INFINITY)
                                             .add("not_a_number", Double.NaN)
                                             .getRecord();

        Meta meta = new Meta().add("foo", Double.POSITIVE_INFINITY).add("bar", Double.NaN).add("baz", Double.NEGATIVE_INFINITY);

        assertJSONEquals(Clip.of(record).add(meta).asJSON(),
                         makeJSON("{'foo': 'Infinity', 'baz': '-Infinity', 'bar': 'NaN'}",
                                 "[{'field': null, 'plus_inf': 'Infinity', 'neg_inf': '-Infinity', 'not_a_number': 'NaN'}]"));
    }

    @Test
    public void testMetadataAddition() {
        Clip clip = new Clip();
        clip.add((Meta) null);
        clip.add(new Meta().add("foo", 1.2));
        assertJSONEquals(clip.asJSON(), makeJSON("{'foo': 1.2}", "[]"));
    }

    @Test
    public void testClipAddition() {
        Clip clip = new Clip();
        clip.add(RecordBox.get().add("foo", "bar").getRecord());

        clip.add((Clip) null);
        assertJSONEquals(clip.asJSON(), makeJSON("[{'foo': 'bar'}]"));

        clip.add(Clip.of(RecordBox.get().add("foo", "bar").getRecord()));
        assertJSONEquals(clip.asJSON(), makeJSON("[{'foo': 'bar'}, {'foo': 'bar'}]"));

        clip.add(Clip.of(new Meta().add("baz", "qux")));
        assertJSONEquals(clip.asJSON(), makeJSON("{'baz': 'qux'}", "[{'foo': 'bar'}, {'foo': 'bar'}]"));
    }
}
