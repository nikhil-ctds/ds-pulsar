package org.apache.pulsar.io.core.transform;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class RenameFields implements Transformation<GenericObject> {
    private static final Logger LOG = LoggerFactory.getLogger(RenameFields.class);

    List<String> sources = new ArrayList<>();
    List<String> targets = new ArrayList<>();

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    class SchemaAndVersion {
        Schema schema;
        byte[] schemaVersion;
        org.apache.avro.Schema keyOrValueAvroSchema;
    }

    @AllArgsConstructor
    private class MyGenericObject implements GenericObject {
        SchemaType schemaType;
        Object nativeObject;

        @Override
        public SchemaType getSchemaType()
        {
            return schemaType;
        }

        @Override
        public Object getNativeObject()
        {
            return nativeObject;
        }

        @Override
        public String toString() {
            return nativeObject.toString();
        }
    }

    @AllArgsConstructor
    private class MyRecord implements Record {
        private final Record record;
        private final Schema schema;
        private final GenericObject genericObject;

        @Override
        public Object getValue() {
            return genericObject;
        }

        @Override
        public Schema getSchema() {
            return schema;
        }

        /**
         * Acknowledge that this record is fully processed.
         */
        public void ack() {
            record.ack();
        }

        /**
         * To indicate that this record has failed to be processed.
         */
        public void fail() {
            record.fail();
        }

        /**
         * To support message routing on a per message basis.
         *
         * @return The topic this message should be written to
         */
        public Optional<String> getDestinationTopic() {
            return record.getDestinationTopic();
        }

        public Optional<Message> getMessage() {
            return record.getMessage();
        }

        @Override
        public String toString() {
            return genericObject.toString();
        }
    }

    // record wrapper with the replaced schema and value
    @AllArgsConstructor
    private class MyKVGenericRecord implements KVRecord
    {
        private final Record record;
        private final Schema keySchema;
        private final Schema valueSchema;
        private final GenericObject genericKeyValue;

        @Override
        public Optional<String> getTopicName() {
            return record.getTopicName();
        }

        @Override
        public Optional<String> getKey() {
            KeyValue keyValue = (KeyValue) genericKeyValue.getNativeObject();
            return keyValue.getKey() == null
            ? Optional.empty()
            : Optional.of(Base64.getEncoder().encodeToString(
                    Schema.BYTES.equals(keySchema.getSchemaInfo().getType()) ? (byte[]) keyValue.getKey() : keySchema.encode(keyValue.getKey())));
        }

        @Override
        public Schema getSchema() {
            return Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.SEPARATED);
        }

        @Override
        public Object getValue() {
            return genericKeyValue;
        }

        /**
         * Acknowledge that this record is fully processed.
         */
        public void ack() {
            record.ack();
        }

        /**
         * To indicate that this record has failed to be processed.
         */
        public void fail() {
            record.fail();
        }

        /**
         * To support message routing on a per message basis.
         *
         * @return The topic this message should be written to
         */
        public Optional<String> getDestinationTopic() {
            return record.getDestinationTopic();
        }

        public Optional<Message> getMessage() {
            return record.getMessage();
        }

        @Override
        public Schema getKeySchema()
        {
            return keySchema;
        }

        @Override
        public Schema getValueSchema()
        {
            return valueSchema;
        }

        @Override
        public KeyValueEncodingType getKeyValueEncodingType()
        {
            return KeyValueEncodingType.SEPARATED;
        }

        @Override
        public String toString() {
            KeyValue keyValue = (KeyValue) genericKeyValue.getNativeObject();
            return keyValue.toString();
        }
    }

    /**
     * Last processed output pulsar Schema
     */
    SchemaAndVersion lastSchemaAndVersion;

    @Override
    public void init(Map<String, Object> config) throws Exception
    {
        if (config.containsKey("renames")) {
            for(String rename : ((String)config.get("renames")).split(",")) {
                String[] parts = rename.split(":");
                if (parts.length == 2 && parts[0].length() > 0 && parts[1].length() > 0) {
                    sources.add(parts[0]);
                    targets.add(parts[1]);
                }
            }
        }
        LOG.warn("rename sources={} targets={}", sources, targets);
    }

    /**
     * Build the output schema
     * @param inputSchema
     * @return
     */
    org.apache.avro.Schema maybeUpdateAvroSchema(org.apache.avro.Schema inputSchema, byte[] schemaVersion) {
        TreeMap<String, org.apache.avro.Schema> props = new TreeMap<>();
        flattenSchema(props, "", inputSchema);
        for(int i = 0; i < sources.size(); i++)
        {
            org.apache.avro.Schema fieldSchema = props.remove(sources.get(i));
            if (fieldSchema != null)
                props.put(targets.get(i), fieldSchema);
        }
        props.remove("");
        return rebuildSchema(inputSchema, new LinkedList<>(props.entrySet()), "", new HashMap<>());
    }

    TreeMap<String, org.apache.avro.Schema> flattenSchema(TreeMap<String, org.apache.avro.Schema> map, String path, org.apache.avro.Schema schema) {
        if (schema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
            map.put(path, schema);
            for (org.apache.avro.Schema.Field field : schema.getFields()) {
                org.apache.avro.Schema fieldSchema = field.schema();
                String key = path.length() > 0 ? path + "." + field.name() : field.name();
                flattenSchema(map, key, fieldSchema);
            }
        } else {
            map.put(path, schema);
        }
        return map;
    }

    org.apache.avro.Schema rebuildSchema(org.apache.avro.Schema rootSchema, List<Map.Entry<String, org.apache.avro.Schema>> entries, String path, Map<String, org.apache.avro.Schema> fields) {
        while(!entries.isEmpty()) {
            Map.Entry<String, org.apache.avro.Schema> entry = entries.get(0);
            if (!entry.getKey().startsWith(path))
                // end node
                break;

            entries.remove(0);
            String subpath = entry.getKey().substring(path.isEmpty() ? 0 : path.length() + 1);
            if (subpath.contains(".")) {
                // start subnode
                String subfield = subpath.substring(0, subpath.indexOf("."));
                fields.put(subfield, rebuildSchema(rootSchema, entries, subfield, new HashMap<>()));
            } else {
                // add field
                fields.put(subpath, entry.getValue().getType().equals(org.apache.avro.Schema.Type.RECORD)
                        ? rebuildSchema(rootSchema, entries, subpath, new HashMap<>())
                        : entry.getValue());
            }
        }
        return org.apache.avro.Schema.createRecord(path.isEmpty() ? "root" : path, "", rootSchema.getNamespace(), rootSchema.isError(),
                fields.entrySet().stream()
                        .map(e -> new org.apache.avro.Schema.Field(e.getKey(), e.getValue()))
                        .collect(Collectors.toList()));
    }

    TreeMap<String, Object> flattenRecord(TreeMap<String, Object> map, String path, org.apache.avro.generic.GenericRecord genericRecord) {
        for(org.apache.avro.Schema.Field field : genericRecord.getSchema().getFields()) {
            Object value = genericRecord.get(field.name());
            String key = path.length() > 0 ? path + "." + field.name() : field.name();
            if (value instanceof org.apache.avro.generic.GenericRecord) {
                flattenRecord(map, key, (org.apache.avro.generic.GenericRecord) value);
            } else {
                map.put(key, value);
            }
        }
        return map;
    }

    org.apache.avro.generic.GenericRecord rebuidRecord(org.apache.avro.Schema schema, List<Map.Entry<String, Object>> entries, String path) {
        org.apache.avro.generic.GenericRecordBuilder genericRecordBuilder = new org.apache.avro.generic.GenericRecordBuilder(schema);
        while(!entries.isEmpty()) {
            Map.Entry<String, Object> entry = entries.get(0);
            if (!entry.getKey().startsWith(path))
                break;

            String subpath = entry.getKey().substring(path.isEmpty() ? 0 : path.length() + 1);
            if (subpath.contains(".")) {
                // add subnode
                String subfield = subpath.substring(0, subpath.indexOf("."));
                genericRecordBuilder.set(subfield, rebuidRecord(schema.getField(subfield).schema(), entries, subfield));
            } else {
                // add field
                entries.remove(0);
                genericRecordBuilder.set(subpath, entry.getValue());
            }
        }
        return genericRecordBuilder.build();
    }

    @Override
    public Record<GenericObject> apply(Record<GenericObject> record) {
        Object object = record.getValue();
        Schema schema = record.getSchema();

        LOG.warn("transforming class={} record={} schema={}", object == null ? null : object.getClass().getName(), object, schema);
        if (object != null && object instanceof org.apache.pulsar.client.api.schema.GenericObject) {
            object = ((org.apache.pulsar.client.api.schema.GenericObject)object).getNativeObject();
        }
        if (schema != null && schema.getNativeSchema().isPresent()) {
            schema = (Schema) schema.getNativeSchema().get();
        }
        LOG.warn("transforming2 class={} record={} schema={}", object == null ? null : object.getClass().getName(), object, schema);

        TreeMap keyProps = new TreeMap<>();
        TreeMap valueProps = new TreeMap<>();
        Schema keySchema = null, valueSchema =  null;
        org.apache.avro.Schema keyAvroSchema = null, valueAvroSchema = null;
        Object key = null, value = null;

        // flatten record
        if (schema != null && schema.getSchemaInfo().getType().equals(SchemaType.KEY_VALUE)) {
            value = object == null ? null : ((KeyValue) object).getValue();
            key = object == null ? record.getKey().get() : ((KeyValue) object).getKey();

            if (key instanceof org.apache.pulsar.client.api.schema.GenericObject) {
                key = ((org.apache.pulsar.client.api.schema.GenericObject) key).getNativeObject();
            }
            if (value != null && value instanceof org.apache.pulsar.client.api.schema.GenericObject) {
                value = ((org.apache.pulsar.client.api.schema.GenericObject) value).getNativeObject();
            }
            LOG.warn("transforming3 valueClass={} value={} keyClass={} key={}",
                    value == null ? null : value.getClass().getName(), value,
                    key == null ? null : key.getClass().getName(), key);

            if (key instanceof GenericData.Record) {
                GenericData.Record inKey = (GenericData.Record) key;
                keyAvroSchema = maybeUpdateAvroSchema(inKey.getSchema(), record.getMessage().isPresent() ? record.getMessage().get().getSchemaVersion() : null);
                flattenRecord(keyProps, "", inKey);
            }
            if (value instanceof GenericData.Record) {
                GenericData.Record inValue = (GenericData.Record) value;
                valueAvroSchema = maybeUpdateAvroSchema(inValue.getSchema(), record.getMessage().isPresent() ? record.getMessage().get().getSchemaVersion() : null);
                flattenRecord(valueProps, "", inValue);
            }
        } else {
            if (object instanceof GenericData.Record) {
                GenericData.Record inValue = (GenericData.Record) object;
                valueAvroSchema = maybeUpdateAvroSchema(inValue.getSchema(), record.getMessage().isPresent() ? record.getMessage().get().getSchemaVersion() : null);
                flattenRecord(valueProps, "", inValue);
            } else {
                // no transformation
                return record;
            }
        }

        // TODO: support copy/move field key <-> value
        // apply key transformations
        for(int i = 0; i < sources.size(); i++) {
            Object v = keyProps.remove(sources.get(i));
            if (v != null)
                keyProps.put(targets.get(i), v);
        }
        // apply value transformations
        for(int i = 0; i < sources.size(); i++) {
            Object v = valueProps.remove(sources.get(i));
            if (v != null)
                valueProps.put(targets.get(i), v);
        }

        // rebuild output
        if (schema != null && schema.getSchemaInfo().getType().equals(SchemaType.KEY_VALUE)) {
            if (key instanceof GenericData.Record) {
                keySchema = new AvroSchemaWrapper(keyAvroSchema);
                key = new MyGenericObject(SchemaType.AVRO, rebuidRecord(keyAvroSchema, new LinkedList<>(keyProps.entrySet()), ""));
            } else {
                keySchema = Schema.BYTES; // TODO: should retrieve the key schema
            }
            if (value instanceof GenericData.Record) {
                valueSchema = new AvroSchemaWrapper(valueAvroSchema);
                value = new MyGenericObject(SchemaType.AVRO, rebuidRecord(valueAvroSchema, new LinkedList<>(valueProps.entrySet()), ""));
            } else {
                valueSchema = Schema.BYTES; // TODO: should retrieve the value schema
            }
            MyKVGenericRecord transformedRecord = new MyKVGenericRecord(record,
                    keySchema, valueSchema, new MyGenericObject(SchemaType.KEY_VALUE, new KeyValue(key, value)));
            LOG.warn("Outpout record={}  class={}", transformedRecord, transformedRecord.getClass().getName());
            return transformedRecord;
        }
        valueSchema = new AvroSchemaWrapper(valueAvroSchema);
        MyRecord genericRecord = new MyRecord(record, valueSchema, new MyGenericObject(SchemaType.AVRO, rebuidRecord(valueAvroSchema, new LinkedList<>(valueProps.entrySet()), "")));
        LOG.warn("Outpout record={}  class={}", genericRecord, genericRecord.getClass().getName());
        return genericRecord;
    }

    @Override
    public boolean test(Record<GenericObject> objectRecord) {
        return true;
    }
}
