package com.alibaba.datax.transformer.utils;

import com.alibaba.fastjson.JSON;

import java.util.Scanner;

public class CoordinateUtils {

    static double M_PI = Math.PI;

    // 墨卡托转经纬度
    private static double[] Mercator2lonLat(double mercatorX, double mercatorY) {
        double[] xy = new double[2];
        double x = mercatorX / 20037508.34 * 180;
        double y = mercatorY / 20037508.34 * 180;
        y = 180 / M_PI * (2 * Math.atan(Math.exp(y * M_PI / 180)) - M_PI / 2);
        xy[0] = x;
        xy[1] = y;
        return xy;
    }

    // 矩形区域转换GeoJson
    public static String geoShapeJson(Double minX, Double minY, Double maxX, Double maxY, boolean isMercator) {
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("{\"type\": \"envelope\", \"coordinates\": [");
            double[] lonLat_min_min = isMercator ? CoordinateUtils.Mercator2lonLat(minX, minY) : new double[]{minX, minY};
            double[] lonLat_max_max = isMercator ? CoordinateUtils.Mercator2lonLat(maxX, maxY) : new double[]{maxX, maxY};
            sb.append('[');
            sb.append(lonLat_min_min[0]);
            sb.append(", ");
            sb.append(lonLat_min_min[1]);
            sb.append(']');
            sb.append(", ");
            sb.append('[');
            sb.append(lonLat_max_max[0]);
            sb.append(", ");
            sb.append(lonLat_max_max[1]);
            sb.append("]]}");
        } catch (Exception exception) {
            return "null";
        }
        return sb.toString();
    }

    public static String geoPointJson(Double x, Double y, boolean isMercator) {
        StringBuilder sb = new StringBuilder();
        try {
            double[] lonLat;
            if (isMercator)
                lonLat = CoordinateUtils.Mercator2lonLat(x, y);
            else
                lonLat = new double[]{x, y};
            sb.append("{\"lon\": ");
            sb.append(lonLat[0]);
            sb.append(", \"lat\": ");
            sb.append(lonLat[1]);
            sb.append('}');
        } catch (Exception exception) {
            return "null";
        }
        return sb.toString();
    }

    public static String wktConvertGeoJson(String wktStr, boolean isMercator) {
        if (wktStr == null || wktStr.contains("EMPTY")) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        try {
            if (wktStr.contains("(((")) {
                // MULTIPOLYGON
                wktStr = wktStr.replaceAll("\\s*MULTIPOLYGON\\s*", "")
                        .replaceAll("\\s*\\(\\(\\(\\s*", "")
                        .replaceAll("\\s*\\)\\)\\)\\s*", "");
                String[] multiPolygonArr = wktStr.split("\\s*\\)\\),\\(\\(\\s*");
                boolean isClosed = true;
                for (int i = 0; i < multiPolygonArr.length; i++) {
                    if (i > 0) sb.append(", ");
                    sb.append('[');
                    String[] polygonArr = multiPolygonArr[i].split("\\s*\\),\\(\\s*");
                    for (int j = 0; j < polygonArr.length; j++) {
                        if (j > 0) sb.append(", ");
                        sb.append('[');
                        isClosed = wktPointsConvert(polygonArr[j], sb, isMercator) == 1 && isClosed;
                        sb.append(']');
                    }
                    sb.append(']');
                }
                if (isClosed)
                    return String.format("{\"type\": \"multipolygon\", \"coordinates\": [%s]}", sb);
                return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
            } else if (wktStr.contains("((")) {
                // POLYGON MULTILINESTRING
                boolean isPolygon = false;
                if (wktStr.contains("POLYGON")) {
                    wktStr = wktStr.replaceAll("\\s*POLYGON\\s*", "");
                    isPolygon = true;
                } else if (wktStr.contains("MULTILINESTRING")) {
                    wktStr = wktStr.replaceAll("\\s*MULTILINESTRING\\s*", "");
                }
                wktStr = wktStr.replaceAll("\\s*\\(\\(\\s*", "")
                        .replaceAll("\\s*\\)\\)\\s*", "");
                String[] polygonArr = wktStr.split("\\s*\\),\\(\\s*");
                boolean isClosed = true;
                for (int i = 0; i < polygonArr.length; i++) {
                    if (i > 0) sb.append(", ");
                    sb.append('[');
                    isClosed = wktPointsConvert(polygonArr[i], sb, isMercator) == 1 && isClosed;
                    sb.append(']');
                }
                if (isPolygon) {
                    if (isClosed)
                        return String.format("{\"type\": \"polygon\", \"coordinates\": [%s]}", sb);
                    return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
                } else
                    return String.format("{\"type\": \"multilinestring\", \"coordinates\": [%s]}", sb);
            } else if (wktStr.contains("(")) {
                // LINESTRING POINT MULTIPOINT BBOX GEOMETRYCOLLECTION
                // 无头部的 LINESTRING 和 MULTIPOINT BBOX 没有明确区分，默认识别为 LINESTRING
                wktStr = wktStr.replaceAll("\\s*\\(\\s*", "")
                        .replaceAll("\\s*\\)\\s*", "");
                if (wktStr.contains("LINESTRING")) {
                    wktStr = wktStr.replaceAll("\\s*LINESTRING\\s*", "");
                    if (wktPointsConvert(wktStr, sb, isMercator) != 0)
                        return String.format("{\"type\": \"linestring\", \"coordinates\": [%s]}", sb);
                    return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
                } else if (wktStr.contains("POINT")) {
                    wktStr = wktStr.replaceAll("\\s*POINT\\s*", "");
                    if (wktPointsConvert(wktStr, sb, isMercator) == 0)
                        return String.format("{\"type\": \"point\", \"coordinates\": %s}", sb);
                    return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
                } else if (wktStr.contains("MULTIPOINT")) {
                    wktStr = wktStr.replaceAll("\\s*MULTIPOINT\\s*", "");
                    if (wktPointsConvert(wktStr, sb, isMercator) != 0)
                        return String.format("{\"type\": \"multipoint\", \"coordinates\": [%s]}", sb);
                    return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
                } else if (wktStr.contains("BBOX")) {
                    wktStr = wktStr.replaceAll("\\s*BBOX\\s*", "");
                    if (wktPointsConvert(wktStr, sb, isMercator) == 4)
                        return String.format("{\"type\": \"envelope\", \"coordinates\": [%s]}", sb);
                    return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
                } else if (wktStr.contains("GEOMETRYCOLLECTION")) {
                    // 形如 https://www.elastic.co/guide/en/elasticsearch/reference/6.2/geo-shape.html#_ulink_url_http_geojson_org_geojson_spec_html_geometrycollection_geometry_collection_ulink
                    // 暂不处理
                    return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
                } else {
                    switch (wktPointsConvert(wktStr, sb, isMercator)) {
                        case 0:
                            return String.format("{\"type\": \"point\", \"coordinates\": %s}", sb);
                        case 1:
                        case 2:
                        case 4:
                            return String.format("{\"type\": \"linestring\", \"coordinates\": [%s]}", sb);
                    }
                    return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
                }
            } else {
                return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
            }
        } catch (Exception exception) {
            return "{\"type\": \"point\", \"coordinates\":[0, 0]}";
        }
    }

    private static byte wktPointsConvert(String pointsWktStr, StringBuilder sb, boolean isMercator) {
        String[] points = pointsWktStr.split("\\s*,\\s*");
        for (int i = 0; i < points.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append('[');
            String[] xyArr = points[i].split("\\s+");
            double[] lonLat = {Double.parseDouble(xyArr[0]), Double.parseDouble(xyArr[1])};
            if (isMercator)
                lonLat = Mercator2lonLat(lonLat[0], lonLat[1]);
            sb.append(lonLat[0]);
            sb.append(", ");
            sb.append(lonLat[1]);
            sb.append(']');
        }
        if (points.length == 1)
            return 0; //单点
        else if (points.length == 2) {
            return 4; //BBOX
        } else {
            if (points[0].equals(points[points.length - 1]))
                return 1; //多点闭合
            else
                return 2; //多点非闭合
        }
    }


    public static void main(String[] args) {
        while (true) {
            Scanner sc = new Scanner(System.in);
            String p = sc.nextLine();
            System.out.println(JSON.parse(wktConvertGeoJson(p, true)));
        }
    }
}