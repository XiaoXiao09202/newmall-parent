package com.xiao.newmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiao.newmall.publisher.service.EsService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName PublisherController
 * @Description TODO
 * @Author WangYiXiao
 * @Date 2020/11/14 22:12
 * @Version 1.0
 **/
@RestController
public class PublisherController {

    @Autowired
    EsService esService;

    @RequestMapping(value = "realtime-total",method = RequestMethod.GET)
    public String realtimeTotal(@RequestParam("date") String dt){

        Long dauTotal = esService.getDauTotal(dt);
        List<Map<String, Object>> rslist = new ArrayList<>();

        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value",dauTotal == null ? 0L : dauTotal);
        rslist.add(dauMap);

        Map<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value",233);
        rslist.add(newMidMap);

        return JSON.toJSONString(rslist);

    }


    @GetMapping("realtime-hour")
    public String realtimeHour(@RequestParam("id") String id,@RequestParam("date") String dt){

        Map dauHourMapTD = esService.getDauHour(dt);
        String yd = getYd(dt);
        Map dauHourMapYD = esService.getDauHour(yd);

        Map<String,Map<String,Long>> rsMap = new HashMap<>();
        rsMap.put("yesterday", dauHourMapYD);
        rsMap.put("today", dauHourMapTD);

        return JSON.toJSONString(rsMap);
    }

    private String getYd(String today){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date todayDate = dateFormat.parse(today);

            Date yesterday = DateUtils.addDays(todayDate, -1);

            String yesterdayDate = dateFormat.format(yesterday);

            return yesterdayDate;

        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式不正确");
        }
    }


}
