package com.atguigu.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //connection.setAutoCommit(true);
    }

    //value:{"database":"", "tableName":"base_trademark", "before":{}, "after":{"id":"", ....}, "type":""}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //每来一条数据  构建一次插入sql语句
            String upsertSQL = generateUpsertSQL(value);
            //测试：检查sql语句
            System.out.println(upsertSQL);

            preparedStatement = connection.prepareStatement(upsertSQL);
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    //value:{"database":"", "tableName":"base_trademark", "before":{}, "after":{"id":"", ....}, "type":""}
    //sql: upsert into database.tableName (column1, column2, ...) values ('value1', 'value1', ...)
    private String generateUpsertSQL(JSONObject value) {
        JSONObject after = value.getJSONObject("after");
        Set<String> keySet = after.keySet();
        Collection<Object> values = after.values();
        String columns = StringUtils.join(keySet, ',');
        String columnValues = StringUtils.join(values, "', '");

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + value.getString("sinkTable") + " (" + columns + ") values ('" + columnValues + "')";
    }
}
