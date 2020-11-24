package com.xiao.newmall.publisher.service;

import java.util.List;
import java.util.Map;

public interface MySQLService {

    // 1 省市地区

    // 2 实时品牌销售额TopN统计
    public List<Map> getTrademarkStat(String startTime,String endTime,int topN);

    // 3 年龄段

    // 4 性别

    // 5 spu

    //6 品类


}
