package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;

import java.util.Arrays;

/**
 * no comments.
 * Created by Rassyan on 18/7/17.
 */
public class JoinFieldTransformer extends Transformer {
    int nameIndex;
    int parentIndex;

    public JoinFieldTransformer() {
        setTransformerName("join_field");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 1) {
                throw new RuntimeException("join_field缺少参数");
            }
            nameIndex = (Integer) paras[0];
            if (nameIndex < 0)
                nameIndex = record.getColumnNumber() + nameIndex;
            parentIndex = nameIndex + 1;
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        try {
            String name = record.getColumn(nameIndex).asString();
            String parent = record.getColumn(parentIndex).asString();
            StringBuilder joinSb = new StringBuilder();
            if (name == null || name.equals("")) {
                throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, "name must not be null");
            }
            joinSb.append("{\"name\":\"");
            joinSb.append(name);
            if (parent != null && !parent.equals("")) {
                joinSb.append("\",\"parent\":\"");
                joinSb.append(parent);
            }
            joinSb.append("\"}");
            record.delColumn(parentIndex);
            record.setColumn(nameIndex, new StringColumn(joinSb.toString()));
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
