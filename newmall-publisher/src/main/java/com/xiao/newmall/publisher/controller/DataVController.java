package com.xiao.newmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.xiao.newmall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName DataVPublisherController
 * @Description TODO
 * @Author WangYiXiao
 * @Date 2020/11/24 13:20
 * @Version 1.0
 **/
@RestController
public class DataVController {

    @Autowired
    MySQLService mySQLService;

    @RequestMapping("trandemarkStat")
    public String queryTrademarkSum(@RequestParam("startTime") String startTime,@RequestParam("endTime") String endTime,@RequestParam("topN") int topN){

        List<Map> mapList = mySQLService.getTrademarkStat(startTime,endTime,topN);
        List<Map> datavFormatList = new ArrayList<>();
        for (Map map : mapList) {
            Map datavMap = new HashMap();
            datavMap.put("x", map.get("trademark_name"));
            datavMap.put("y", map.get("order_amount"));
            datavMap.put("s", "1");
            datavFormatList.add(datavMap);
        }

        return JSON.toJSONString(datavFormatList);
    }
}
