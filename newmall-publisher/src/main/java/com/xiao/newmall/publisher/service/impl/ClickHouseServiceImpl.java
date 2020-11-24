package com.xiao.newmall.publisher.service.impl;

import com.xiao.newmall.publisher.mapper.OrderWideMapper;
import com.xiao.newmall.publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName ClickHouseServiceImpl
 * @Description TODO
 * @Author WangYiXiao
 * @Date 2020/11/23 17:08
 * @Version 1.0
 **/
@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Autowired
    public OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderAmount(String date) {

        return orderWideMapper.selectOrderAmount(date);

    }

    @Override
    public Map getOrderAmoutHour(String date) {

        //转换格式
        List<Map> mapList = orderWideMapper.selectOrderAmountHour(date);

        Map<String,BigDecimal> hourMap = new HashMap<>();

        for (Map map : mapList) {
            hourMap.put(String.valueOf(map.get("hr")),(BigDecimal)map.get("order_amount"));
        }

        return hourMap;
    }
}
