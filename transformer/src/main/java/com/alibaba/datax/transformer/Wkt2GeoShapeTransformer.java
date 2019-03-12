package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.utils.CoordinateUtils;

import java.util.Arrays;

/**
 * no comments.
 * Created by Rassyan on 18/5/15.
 */
public class Wkt2GeoShapeTransformer extends Transformer {
    int wktIndex;
    boolean isMercator;

    public Wkt2GeoShapeTransformer() {
        setTransformerName("wkt2geo_shape");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 1) {
                throw new RuntimeException("wkt2geo_shape transformer缺少参数");
            }
            wktIndex = (Integer) paras[0];
            if (wktIndex < 0)
                wktIndex = record.getColumnNumber() + wktIndex;
            isMercator = paras.length > 1 && "mercator".equals(paras[1]);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column wktColumn = record.getColumn(wktIndex);
        try {
            String wkt = wktColumn.asString();
            if (wkt == null) {
                return record;
            }
            if (wktColumn.getType() == Column.Type.STRING) {
                record.setColumn(wktIndex, new StringColumn(CoordinateUtils.wktConvertGeoJson(wkt, isMercator)));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
