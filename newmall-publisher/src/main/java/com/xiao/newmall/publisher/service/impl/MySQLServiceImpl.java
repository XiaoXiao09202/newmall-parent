package com.xiao.newmall.publisher.service.impl;

import com.xiao.newmall.publisher.mapper.TrademarkStatMapper;
import com.xiao.newmall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @ClassName MySQLServiceImpl
 * @Description TODO
 * @Author WangYiXiao
 * @Date 2020/11/24 13:14
 * @Version 1.0
 **/
@Service
public class MySQLServiceImpl implements MySQLService {

    @Autowired
    TrademarkStatMapper trademarkMapper;

    @Override
    public List<Map> getTrademarkStat(String startTime, String endTime, int topN) {

        return trademarkMapper.selectTrademarkSum(startTime, endTime, topN);

    }
}
