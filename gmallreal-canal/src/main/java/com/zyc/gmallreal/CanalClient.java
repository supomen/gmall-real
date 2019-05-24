package com.zyc.gmallreal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;

public class CanalClient {


    public static void startConnect( ) {
        //destinationcanal serverinstanceexample
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop101", 11111), "example", "", "");
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmallreal.order_info");   //
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();
            if (size == 0) {
                System.out.println("5");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONBEGIN) || entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONEND)) {
                        continue;
                    }
                    CanalEntry.RowChange rowChange = null;
                    try {
                        rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        EventHandler.handleEvent(entry.getHeader().getTableName(), rowChange.getEventType(), rowChange.getRowDatasList());
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }

                }
            }
        }
    }
}
