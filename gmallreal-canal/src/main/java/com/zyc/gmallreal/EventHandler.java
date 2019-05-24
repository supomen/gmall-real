package com.zyc.gmallreal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.protocol.CanalEntry;
import constant.GmallConstant;

import java.util.List;

public class EventHandler {


    /**
     * 根据表名和事件类型，来发送kafka
     * @param tableName
     * @param eventType
     * @param rowDatasList
     */
    public static  void  handleEvent(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList ){
        if("order_info".equals(tableName)&& CanalEntry.EventType.INSERT==eventType){ //下单
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();

                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : columnsList) {
                    String columnName = column.getName();
                    String columnValue = column.getValue();
                    String propertiesName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    jsonObject.put(propertiesName,columnValue);
                }
                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_NEW_ORDER,jsonObject.toString());
            }

        }
    }
}


