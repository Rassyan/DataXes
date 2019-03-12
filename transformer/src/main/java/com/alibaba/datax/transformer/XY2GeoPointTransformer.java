package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.utils.CoordinateUtils;

import java.util.Arrays;

/**
 * no comments.
 * Created by Rassyan on 18/7/17.
 */
public class XY2GeoPointTransformer extends Transformer {
    int xIndex;
    int yIndex;
    boolean isMercator;

    public XY2GeoPointTransformer() {
        setTransformerName("xy2geo_point");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 1) {
                throw new RuntimeException("xy2geo_point缺少参数");
            }
            xIndex = (Integer) paras[0];
            if (xIndex < 0)
                xIndex = record.getColumnNumber() + xIndex;
            yIndex = xIndex + 1;
            isMercator = paras.length > 1 && "mercator".equals(paras[1]);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column xColumn = record.getColumn(xIndex);
        Column yColumn = record.getColumn(yIndex);
        try {
            Double x = xColumn.asDouble();
            Double y = yColumn.asDouble();
            record.delColumn(yIndex);
            if (x == null || y == null) {
                record.setColumn(xIndex, new StringColumn(null));
            } else {
                record.setColumn(xIndex, new StringColumn(CoordinateUtils.geoPointJson(x, y, isMercator)));
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
