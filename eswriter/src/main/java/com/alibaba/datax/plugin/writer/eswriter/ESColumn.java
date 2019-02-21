package com.alibaba.datax.plugin.writer.eswriter;

/**
 * Created by xiongfeng.bxf on 17/3/2.
 */
public class ESColumn {

    private String name;

    private String type;

    private String timezone;

    private String format;

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getFormat() {
        return format;
    }
}
