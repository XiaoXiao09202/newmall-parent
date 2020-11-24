package com.xiao.newmall.publisher.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @ClassName EsService
 * @Description TODO
 * @Author WangYiXiao
 * @Date 2020/11/14 22:12
 * @Version 1.0
 **/
public interface EsService {

    public Long getDauTotal(String date);

    public Map getDauHour(String date);

}
