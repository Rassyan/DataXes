package com.alibaba.datax.plugin.writer.eswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ESWriterErrorCode implements ErrorCode {
    BAD_CONFIG_VALUE("ESWriter-00", "您配置的值不合法."),
    ES_INDEX_INSERT("ESWriter-01", "插入数据错误."),
    ES_BULK_CREATE("ESWriter-02", "bulk构造错误."),
    ES_SCRIPT("ESWriter-03", "Script缺失."),
    ES_FIELDTYPE_PARSE("ESWriter-04", "解析field type错误."),
    ;

    private final String code;
    private final String description;

    ESWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}