package com.alibaba.datax.plugin.writer.eswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.Feature;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ESWriter extends Writer {
    private final static String WRITE_COLUMNS = "write_columns";
    private final static String TYPE_DEFAULT_NAME = "_doc";

    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            // 强制fastJson不使用BigDecimal
            int features = 0;
            features |= Feature.AutoCloseSource.getMask();
            features |= Feature.InternFieldNames.getMask();
//            features |= Feature.UseBigDecimal.getMask();
            features |= Feature.AllowUnQuotedFieldNames.getMask();
            features |= Feature.AllowSingleQuotes.getMask();
            features |= Feature.AllowArbitraryCommas.getMask();
            features |= Feature.SortFeidFastMatch.getMask();
            features |= Feature.IgnoreNotMatch.getMask();
            JSON.DEFAULT_PARSER_FEATURE = features;
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
            List<ESColumn> columnList = new ArrayList<>();
            List column = conf.getList("column");
            if (column != null) {
                for (Object col : column) {
                    JSONObject jo = JSONObject.parseObject(col.toString());
                    String colName = jo.getString("name");
                    String colTypeStr = jo.getString("type");
                    if (colTypeStr == null) {
                        throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + " column must have type");
                    }
                    ESFieldType colType = ESFieldType.getESFieldType(colTypeStr);
                    if (colType == null) {
                        throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + " unsupported type");
                    }
                    ESColumn columnItem = new ESColumn();
                    columnItem.setName(colName);
                    columnItem.setType(colTypeStr);
                    columnList.add(columnItem);
                }
            }
            conf.set(WRITE_COLUMNS, JSON.toJSONString(columnList));
            log.info(JSON.toJSONString(columnList));
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private Configuration conf;
        private long total;
        private long byQueryTotal;

        ESClient esClient = null;
        private List<ESFieldType> typeList;
        private List<ESColumn> columnList;

        private int bulkActions;
        private int bulkSizeMB;
        private int retryDelaySecs;
        private int maxNumberOfRetries;
        private String index;
        private String type = TYPE_DEFAULT_NAME;

        private Key.ActionType actionType;

        @Override
        public void init() {
            this.total = 0;
            this.byQueryTotal = 0;
            this.conf = super.getPluginJobConf();
            index = Key.getIndexName(conf);

            bulkActions = Key.getBulkActions(conf);
            bulkSizeMB = Key.getBulkSizeMB(conf);
            retryDelaySecs = Key.getRetryDelaySecs(conf);
            maxNumberOfRetries = Key.getMaxNumberOfRetries(conf);
            columnList = JSON.parseObject(this.conf.getString(WRITE_COLUMNS), new TypeReference<List<ESColumn>>() {
            });

            typeList = new ArrayList<>();

            for (ESColumn col : columnList) {
                typeList.add(ESFieldType.getESFieldType(col.getType()));
            }

            esClient = new ESClient();

            actionType = Key.getActionType(conf);
        }

        @Override
        public void prepare() {
            esClient.createClient(Key.getHosts(conf));
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    int numberOfActions = request.numberOfActions();
                    log.debug("Executing bulk [{}] with {} requests",
                            executionId, numberOfActions);
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    if (response.hasFailures()) {
                        log.warn("Bulk [{}] executed with failures", executionId);
                        for (int i = 0; i < response.getItems().length; i++) {
                            BulkItemResponse bulkItemResponse = response.getItems()[i];
                            if (bulkItemResponse.isFailed()) {
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                if (Key.isIgnoreWriteError(conf)) {
                                    // 由于使用BulkProcessor，无法获得失败Record数据的原始对象，改用es抛错信息充当
                                    Record dirtyRecord = new Record() {
                                        private List<Column> columns = new ArrayList<>();
                                        private int byteSize;

                                        @Override
                                        public void addColumn(Column column) {
                                            columns.add(column);
                                            byteSize += column.getByteSize();
                                        }

                                        @Override
                                        public void addColumn(int i, Column column) {

                                        }

                                        @Override
                                        public void setColumn(int i, Column column) {

                                        }

                                        @Override
                                        public Column getColumn(int i) {
                                            return null;
                                        }

                                        @Override
                                        public int getColumnNumber() {
                                            return this.columns.size();
                                        }

                                        @Override
                                        public int getByteSize() {
                                            return byteSize;
                                        }

                                        @Override
                                        public int getMemorySize() {
                                            return 0;
                                        }

                                        @Override
                                        public void delColumn(int i) {

                                        }

                                        @Override
                                        public String toString() {
                                            Map<String, Object> json = new HashMap<String, Object>();
                                            json.put("size", this.getColumnNumber());
                                            json.put("data", this.columns);
                                            return JSON.toJSONString(json);
                                        }
                                    };
                                    dirtyRecord.addColumn(new StringColumn(request.requests().get(i).toString()));
                                    getTaskPluginCollector().collectDirtyRecord(dirtyRecord, failure.toString());
                                    log.warn(String.format("重试[%d]次写入失败，忽略该错误，继续写入!", maxNumberOfRetries));
                                } else {
                                    throw DataXException.asDataXException(ESWriterErrorCode.ES_INDEX_INSERT, bulkItemResponse.getFailure().getCause());
                                }
                            } else
                                total++;
                        }
                    } else
                        total += response.getItems().length;
                    log.debug("Bulk [{}] completed in {} milliseconds",
                            executionId, response.getTook().getMillis());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    if (Key.isIgnoreWriteError(conf)) {
                        log.error("Failed to execute bulk", failure);
                    } else {
                        throw DataXException.asDataXException(ESWriterErrorCode.ES_INDEX_INSERT, failure);
                    }
                }
            };
            BulkProcessor bulkProcessor = BulkProcessor.builder(esClient.getClient()::bulkAsync, listener)
                    .setBulkActions(this.bulkActions)
                    .setBulkSize(new ByteSizeValue(bulkSizeMB, ByteSizeUnit.MB))
                    .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(retryDelaySecs), maxNumberOfRetries))
                    .build();

            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                StringBuilder idStringBuilder = new StringBuilder();
                StringBuilder indexStringBuilder = new StringBuilder();
                StringBuilder routingStringBuilder = new StringBuilder();
                StringBuilder queryStringBuilder = new StringBuilder();
                Map<String, Object> jsonMap = toEsJsonMap(record, idStringBuilder, indexStringBuilder, routingStringBuilder, queryStringBuilder);
                String _id = idStringBuilder.toString();
                String _index = indexStringBuilder.toString();
                if (_index.equals("")) {
                    _index = index;
                }
                String _routing = routingStringBuilder.toString();
                String _query = queryStringBuilder.toString();

                String scriptString = Key.getScript(conf);
                Script _script = null;
                if (scriptString != null) {
                    _script = new Script(ScriptType.INLINE, "painless", scriptString, jsonMap);
                }

                switch (actionType) {
                    case INDEX:
                        IndexRequest indexRequest = ((_id.equals("")) ?
                                new IndexRequest(_index, type) :
                                new IndexRequest(_index, type, _id)).source(jsonMap);
                        if (!_routing.equals("")) {
                            indexRequest.routing(_routing);
                        }
                        bulkProcessor.add(indexRequest);
                        break;
                    case DELETE:
                        if (_id.equals("")) {
                            throw DataXException.asDataXException(ESWriterErrorCode.ES_BULK_CREATE, "delete action need a given id");
                        }
                        DeleteRequest deleteRequest = new DeleteRequest(_index, type, _id);
                        if (!_routing.equals("")) {
                            deleteRequest.routing(_routing);
                        }
                        bulkProcessor.add(deleteRequest);
                        break;
                    case UPDATE:
                        if (_id.equals("")) {
                            throw DataXException.asDataXException(ESWriterErrorCode.ES_BULK_CREATE, "update action need a given id");
                        }
                        UpdateRequest updateRequest = new UpdateRequest(_index, type, _id).doc(jsonMap);
                        if (!_routing.equals("")) {
                            updateRequest.routing(_routing);
                        }
                        bulkProcessor.add(updateRequest);
                        break;
                    case SCRIPTED_UPDATE:
                        if (_id.equals("")) {
                            throw DataXException.asDataXException(ESWriterErrorCode.ES_BULK_CREATE, String.format("scripted_update action need a given id"));
                        }
                        if (_script == null) {
                            throw DataXException.asDataXException(ESWriterErrorCode.ES_SCRIPT, String.format("scripted_update action need a script"));
                        }
                        UpdateRequest scriptedUpdateRequest = new UpdateRequest(_index, type, _id).script(_script);
                        if (!_routing.equals("")) {
                            scriptedUpdateRequest.routing(_routing);
                        }
                        bulkProcessor.add(scriptedUpdateRequest);
                        break;
                    case UPSERT:
                        if (_id.equals("")) {
                            throw DataXException.asDataXException(ESWriterErrorCode.ES_BULK_CREATE, String.format("upsert action need a given id"));
                        }
                        UpdateRequest upsertRequest = new UpdateRequest(_index, type, _id).doc(jsonMap).docAsUpsert(true);
                        if (_script != null) {
                            upsertRequest = new UpdateRequest(_index, type, _id).script(_script).upsert(jsonMap);
                        }
                        if (!_routing.equals("")) {
                            upsertRequest.routing(_routing);
                        }
                        bulkProcessor.add(upsertRequest);
                        break;
                    case SCRIPTED_UPSERT:
                        if (_id.equals("")) {
                            throw DataXException.asDataXException(ESWriterErrorCode.ES_BULK_CREATE, String.format("upsert action need a given id"));
                        }
                        if (_script == null) {
                            throw DataXException.asDataXException(ESWriterErrorCode.ES_SCRIPT, String.format("scripted_upsert action need a script"));
                        }
                        UpdateRequest scriptedUpsertRequest = new UpdateRequest(_index, type, _id).script(_script).scriptedUpsert(true);
                        if (!_routing.equals("")) {
                            scriptedUpsertRequest.routing(_routing);
                        }
                        bulkProcessor.add(scriptedUpsertRequest);
                        break;
                    case DELETE_BY_QUERY:
                        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(_index);
                        deleteByQueryRequest.setQuery(QueryBuilders.wrapperQuery(_query));
                        deleteByQueryRequest.setConflicts("proceed");
                        if (!_routing.equals("")) {
                            deleteByQueryRequest.setRouting(_routing);
                        }
                        try {
                            BulkByScrollResponse bulkResponse = esClient.getClient().deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
                            total++;
                            byQueryTotal += bulkResponse.getTotal();
                        } catch (IOException e) {
                            getTaskPluginCollector().collectDirtyRecord(record, e, deleteByQueryRequest.toString());
                        }
                        break;
                    case UPDATE_BY_QUERY:
                        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(_index);
                        updateByQueryRequest.setQuery(QueryBuilders.wrapperQuery(_query));
                        updateByQueryRequest.setScript(_script);
                        updateByQueryRequest.setConflicts("proceed");
                        if (!_routing.equals("")) {
                            updateByQueryRequest.setRouting(_routing);
                        }
                        try {
                            BulkByScrollResponse bulkResponse = esClient.getClient().updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
                            total++;
                            byQueryTotal += bulkResponse.getTotal();
                        } catch (IOException e) {
                            getTaskPluginCollector().collectDirtyRecord(record, e, updateByQueryRequest.toString());
                        }
                        break;
                    case UNKOWN:
                    default:
                        throw DataXException.asDataXException(ESWriterErrorCode.ES_BULK_CREATE, String.format("unknown action"));
                }
            }
            bulkProcessor.flush();

            String msg = String.format("task end, total count :%d", total);
            log.info(msg);
            if (byQueryTotal > 0) {
                msg = String.format("task end, total count by query :%d", byQueryTotal);
                log.info(msg);
            }
            try {
                bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
                esClient.closeClient();
            } catch (Exception e) {
                log.error("Error occurred when close es client", e);
            }
        }

        private String getDateStr(ESColumn esColumn, Column column) {
            DateTime date;
            DateTimeZone dtz = DateTimeZone.getDefault();
            if (esColumn.getTimezone() != null) {
                // 所有时区参考 http://www.joda.org/joda-time/timezones.html
                dtz = DateTimeZone.forID(esColumn.getTimezone());
            }
            if (column.getType() != Column.Type.DATE && esColumn.getFormat() != null) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(esColumn.getFormat());
                date = formatter.withZone(dtz).parseDateTime(column.asString());
                return date.toString();
            } else if (column.getType() == Column.Type.DATE) {
                if (column.asLong() == null) {
                    return null;
                }
                date = new DateTime(column.asLong(), dtz);
                return date.toString();
            } else {
                return column.asString();
            }
        }

        private Map<String, Object> toEsJsonMap(final Record record, StringBuilder _id, StringBuilder _index, StringBuilder _routing, StringBuilder _query) {
            Map<String, Object> data = new HashMap<>();
            String nextColumnName = null;
            for (int i = 0; i < record.getColumnNumber(); i++) {
                Column column = record.getColumn(i);
                String columnName = nextColumnName == null ? columnList.get(i).getName() : nextColumnName;
                nextColumnName = null;
                if (column.getRawData() == null) continue;
                ESFieldType columnType = typeList.get(i);
                switch (columnType) {
                    case ID:
                        _id.append(record.getColumn(i).asString());
                        break;
                    case INDEX:
                        _index.append(record.getColumn(i).asString());
                        break;
                    case ROUTING:
                        _routing.append(record.getColumn(i).asString());
                        break;
                    case QUERY:
                        _query.append(record.getColumn(i).asString());
                        break;
                    case NEXT_COLUMN_NAME:
                        nextColumnName = record.getColumn(i).asString();
                        break;

                    case DATE:
                        try {
                            String dateStr = getDateStr(columnList.get(i), column);
                            data.put(columnName, dateStr);
                        } catch (Exception e) {
                            getTaskPluginCollector().collectDirtyRecord(record, String.format("时间类型解析失败 [%s:%s] exception: %s", columnName, column.toString(), e.toString()));
                            if (!Key.isIgnoreParseError(conf)) throw DataXException.asDataXException(ESWriterErrorCode.ES_FIELDTYPE_PARSE, String.format("时间类型解析失败"));
                        }
                        break;
                    case KEYWORD:
                    case STRING:
                    case TEXT:
                    case IP:
                        data.put(columnName, column.asString());
                        break;
                    case BOOLEAN:
                        data.put(columnName, column.asBoolean());
                        break;
                    case BYTE:
                    case BINARY:
                        data.put(columnName, column.asBytes());
                        break;
                    case LONG:
                    case INTEGER:
                    case SHORT:
                        data.put(columnName, column.asLong());
                        break;
                    case FLOAT:
                    case DOUBLE:
                        data.put(columnName, column.asDouble());
                        break;
                    case ARRAY:
                    case OBJECT:
                    case NESTED:
                    case GEO_SHAPE:
                    case GEO_POINT:
                        try {
                            data.put(columnName, JSON.parse(column.asString()));
                        } catch (Exception e) {
                            getTaskPluginCollector().collectDirtyRecord(record, String.format("JSON转换失败 [%s:%s] exception: %s", columnName, column.toString(), e.toString()));
                            if (!Key.isIgnoreParseError(conf)) throw DataXException.asDataXException(ESWriterErrorCode.ES_FIELDTYPE_PARSE, String.format("JSON转换失败"));
                        }
                        break;
                    default:
                        getTaskPluginCollector().collectDirtyRecord(record, "类型错误:不支持的类型:" + columnType + " " + columnName);
                        if (!Key.isIgnoreParseError(conf)) throw DataXException.asDataXException(ESWriterErrorCode.ES_FIELDTYPE_PARSE, String.format("unknown type"));
                }
            }
            return data;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            try {
                esClient.closeClient();
            } catch (Exception e) {
                log.error("Error occurred when close es client", e);
            }
        }
    }
}
