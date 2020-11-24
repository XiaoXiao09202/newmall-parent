package com.xiao.newmall.publisher.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ClickHouseService {

    public BigDecimal getOrderAmount(String date);

    public Map getOrderAmoutHour(String date);

}
