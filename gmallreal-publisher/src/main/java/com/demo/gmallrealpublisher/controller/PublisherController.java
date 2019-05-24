package com.demo.gmallrealpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.demo.gmallrealpublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){

        List<Map> totalList = new ArrayList<>();
        HashMap<Object, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value", dauTotal);
        totalList.add(dauMap);

        HashMap<Object, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
//        Integer newMid = publisherService.getNewMid(date);
        newMidMap.put("value", 233);
        totalList.add(newMidMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date){

        Map<Object, Object> aduHourMap = new HashMap<>();

        if ("dau".equals(id)){
            Map dauHourMapToday = publisherService.getDauHour(date);
            Map dauHourMapYesterday = publisherService.getDauHour(getYesterday(date));
            aduHourMap.put("yesterday",dauHourMapYesterday);
            aduHourMap.put("today",dauHourMapToday);
        }else{
            System.out.println("请输入正确的id");
        }

        return JSON.toJSONString(aduHourMap);
    }

    public String getYesterday(String today){
        Date todayDT = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            todayDT = simpleDateFormat.parse(today);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date yesterdayDt = DateUtils.addDays(todayDT, -1);
        String yesterday = simpleDateFormat.format(yesterdayDt);
        return yesterday;

    }
}
