package com.alibaba.datax.plugin.writer.eswriter;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static com.alibaba.datax.plugin.writer.eswriter.Key.ActionType.UNKOWN;

public final class Key {
    public enum ActionType {
        UNKOWN,
        INDEX,
        DELETE,
        UPDATE,                 // Updates with a partial document，id存在则局部更新，否则请求报错并作废
        SCRIPTED_UPDATE,        // Scripted updates, 同上，此时doc作为params供脚本取用
        UPSERT,                 // 如果配置了script将遵循「id存在则SCRIPTED_UPDATE，不存在则INDEX」，否则「id存在则UPDATE，不存在则INDEX」
        SCRIPTED_UPSERT,        // id存在与否都将执行Scripted updates
        DELETE_BY_QUERY,
        UPDATE_BY_QUERY
    }

    public static ActionType getActionType(Configuration conf) {
        String actionType = conf.getString("action_type", "index");
        {
            if (actionType == null) {
                return UNKOWN;
            }
            for (ActionType f : ActionType.values()) {
                if (f.name().compareTo(actionType.toUpperCase()) == 0) {
                    return f;
                }
            }
            return UNKOWN;
        }
    }

    public static int getBulkActions(Configuration conf) {
        return conf.getInt("bulk_actions", 5000);
    }

    public static int getBulkSizeMB(Configuration conf) {
        return conf.getInt("bulk_size_mb", 20);
    }

    public static int getRetryDelaySecs(Configuration conf) {
        return conf.getInt("retry_delay_secs", 1);
    }

    public static int getMaxNumberOfRetries(Configuration conf) {
        return conf.getInt("max_number_of_retries", 3);
    }

    public static String getIndexName(Configuration conf) {
        return conf.getNecessaryValue("index", ESWriterErrorCode.BAD_CONFIG_VALUE);
    }

    public static boolean isIgnoreWriteError(Configuration conf) {
        return conf.getBool("ignoreWriteError", true);
    }

    public static boolean isIgnoreParseError(Configuration conf) {
        return conf.getBool("ignoreParseError", true);
    }

    public static List<String> getHosts(Configuration conf) {
        return conf.getList(
                "hosts", Collections.<String>emptyList(), String.class);
    }

    public static String getScript(Configuration conf) {
        return conf.getString("script");
    }
}
