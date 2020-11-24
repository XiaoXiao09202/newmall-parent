package com.xiao.newmall.realtime.dws

import java.util.Properties
import java.{lang, util}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.xiao.newmall.realtime.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.xiao.newmall.realtime.util.{OffsetManager, XxKafkaSink, XxKafkaUtil, XxRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderDetailWideApp {

  def main(args: Array[String]): Unit = {
    //===加载流====
    val sparkConf = new SparkConf().setAppName("order_wide_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topicOrderInfo = "DWD_ORDER_INFO"
    val groupIdOrderInfo = "dws_order_info_group"
    val topicOrderDetail = "DWD_ORDER_DETAIL"
    val groupIdOrderDetail = "dws_order_detail_group"

    //==== 订单主表 ====
    val orderInfoKafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderInfo,groupIdOrderInfo)
    var orderInfoRecordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderInfoKafkaOffsetMap != null && orderInfoKafkaOffsetMap.size>0){
      orderInfoRecordInputStream = XxKafkaUtil.getKafkaStream(topicOrderInfo,ssc,orderInfoKafkaOffsetMap,groupIdOrderInfo)
    }else{
      orderInfoRecordInputStream = XxKafkaUtil.getKafkaStream(topicOrderInfo,ssc,groupIdOrderInfo)
    }
    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var orderInfoOffsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputStream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver端 周期性执行
      rdd
    }

    //===== 订单明细 ====
    val orderDetailKafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderDetail,groupIdOrderDetail)
    var orderDetailRecordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderDetailKafkaOffsetMap != null && orderDetailKafkaOffsetMap.size>0){
      orderDetailRecordInputStream = XxKafkaUtil.getKafkaStream(topicOrderDetail,ssc,orderDetailKafkaOffsetMap,groupIdOrderDetail)
    }else{
      orderDetailRecordInputStream = XxKafkaUtil.getKafkaStream(topicOrderDetail,ssc,groupIdOrderDetail)
    }
    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var OrderDetailOffsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputStream.transform { rdd =>
      OrderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver端 周期性执行
      rdd
    }

    //==== 结构调整 ====
    //转化为OrderInfo
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo
    }

    //转化为OrderDetail
    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    }

//    orderInfoDstream.cache()
//    orderInfoDstream.print(1000)
//    orderDetailDstream.cache()
//    orderDetailDstream.print(1000)


    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

     //第一种办法 开窗去重
    //======= 双流join ========
    //无法保证应该配对的主表和从表数据都在一个批次中，join有可能丢失数据
    //val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)

    //1 开窗口
    val orderInfoWithKeyWindowDstream: DStream[(Long, OrderInfo)] = orderInfoWithKeyDstream.window(Seconds(10),Seconds(5))
    val orderDetailWithKeyWindowDstream: DStream[(Long, OrderDetail)] = orderDetailWithKeyDstream.window(Seconds(10),Seconds(5))

    //2 join
    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyWindowDstream.join(orderDetailWithKeyWindowDstream)
//    orderJoinedDstream.cache()
//    orderJoinedDstream.print(1000)
    //3 去重
    val orderJoinedNewDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderJoinedDstream.mapPartitions { orderJoinedTupleItr =>

      val jedis: Jedis = XxRedisUtil.getJedisClient
      val key = "order_join_keys"
      val orderJoinedNewList = new ListBuffer[(Long, (OrderInfo, OrderDetail))]()

      for ((orderId, (orderInfo, orderDetail)) <- orderJoinedTupleItr) {
        //Redis   type?  set     key  order_join_keys      value  orderDetail.id
        val ifNew: lang.Long = jedis.sadd(key, orderDetail.id.toString)
        if (ifNew == 1L) {
          orderJoinedNewList.append((orderId, (orderInfo, orderDetail)))
        }
      }
      jedis.close()
      orderJoinedNewList.toIterator
    }
//    orderJoinedNewDstream.cache()
//    orderJoinedNewDstream.print(1000)

    val orderDetailWideDstream: DStream[OrderDetailWide] = orderJoinedNewDstream.map{case(orderId,(orderInfo,orderDetail))=>new OrderDetailWide(orderInfo,orderDetail)}
//    orderDetailWideDstream.cache()
//    orderDetailWideDstream.print(1000)

    /*//第二种办法
    val orderFullJoinedDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)

    val saleDetailDstream: DStream[OrderDetailWide] = orderFullJoinedDstream.flatMap { case (orderId, (orderInfoOption, orderDetailOption)) =>
      //1主表部分
      val orderDetailWideList = new ListBuffer[OrderDetailWide]
      val jedis: Jedis = XxRedisUtil.getJedisClient
      if (orderInfoOption != None) {
        val orderInfo: OrderInfo = orderInfoOption.get
        //1.1 在同一批次能够关联 两个对象组合成一个新的宽表对象
        if (orderDetailOption != None) {
          val orderDetail: OrderDetail = orderDetailOption.get
          val orderDetailWide = new OrderDetailWide(orderInfo, orderDetail)
          orderDetailWideList.append(orderDetailWide)
        }
        //1.2 转换成json写入缓存
        val orderInfoJson: String = JSON.toJSONString(orderInfo,new SerializeConfig(true))
        // redis 写入  type ? string list set zset hash    key?  order_info:[order_id]    value?  orderInfoJson    expire?
        //为什么不用集合 比如 hash 来存储整个的orderInfo 清单呢
        //1 没必要 因为不需要一下取出整个清单
        //2 超大hash 不容易进行分布式
        //3 hash 中的单独k-v  没法设定过期时间
        val orderInfoKey = "order_info:" + orderInfo.id
        jedis.setex(orderInfoKey, 600, orderInfoJson)
        //1.3 查询缓存中是否有对应的orderDetail
        val orderDetailKey = "order_detail:" + orderInfo.id
        val orderDetailJsonSet: util.Set[String] = jedis.smembers(orderDetailKey)
        if (orderDetailJsonSet != null && orderDetailJsonSet.size() > 0) {
          import scala.collection.JavaConversions._
          for (orderDetailJsonString <- orderDetailJsonSet) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonString, classOf[OrderDetail])
            val orderDetailWide = new OrderDetailWide(orderInfo, orderDetail)
            orderDetailWideList.append(orderDetailWide)
          }
        }
      } else { //2 从表
        val orderDetail: OrderDetail = orderDetailOption.get
        //2.1 转换成json写入缓存
        val orderDetailJson: String = JSON.toJSONString(orderDetail,new SerializeConfig(true))
        //Redis ? type?  set   key?   order_detail:[order_id]    value? orderDetailJsons
        val orderDetailKey = "order_detail:" + orderDetail.order_id
        jedis.sadd(orderDetailKey, orderDetailJson)
        jedis.expire(orderDetailKey, 600)

        //2.2 从表查询缓存中主表信息
        val orderInfoKey = "order_info:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        if (orderInfoJson != null && orderInfoJson.length > 0) {
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
          orderDetailWideList.append(new OrderDetailWide(orderInfo, orderDetail))
        }
      }
      jedis.close()
      orderDetailWideList
    }
    saleDetailDstream.print(1000)*/


    ////////////////////////////////////////////////////////////
    ////////////////////计算实付分摊需求////////////////////////
    ////////////////////////////////////////////////////////////
    //思路：
    //每条明细已有 1 原始总金额（original_total_amount）(明细单价和各个数的汇总值)
    //             2 实付总金额（final_total_amount）原始金额 - 优惠金额 + 运费
    //             3 购买数量  （sku_num）
    //             4 单价       (order_price)
    //
    //  求  每条明细的实付分摊金额（按照明细消费金额比例拆分）
    //
    //   1  33,33   40    120
    //   2  33.33   40    120
    //   3  ?       40    120


    //  如果  计算是该明细不是最后一笔
    //  使用乘除法       实付分摊金额/实付总金额 = （单价*数量）/原始总金额
    //  调整移项可得     实付分摊金额=（单价*数量）*实付总金额 / 原始总金额
    //
    //  如果  计算时该明细是最后一笔
    //  使用减法         实付分摊金额 = 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
    //   1   减法公式
    //   2   如何判断是最后一笔
    //       如果  该条明细 （单价*数量）== 原始总金额 - （其他明细 【单价*数量】的合计）
    //
    //
    //   两个合计值 如何处理
    //   在依次计算的过程中 把  订单的已经计算完的明细的【实付分摊金额】的合计
    //   经典的已经计算完的明细的【单价*数量】的合计
    //   保存在redis中 key设计


    val orderWideWithSplitDstream: DStream[OrderDetailWide] = orderDetailWideDstream.mapPartitions { orderwideItr =>
      val jedis: Jedis = XxRedisUtil.getJedisClient
      //  1 先从redis取  两个合计     【实付分摊金额】的合计  【单价*数量】的合计
      val orderWideList: List[OrderDetailWide] = orderwideItr.toList
      for (orderWide <- orderWideList) {
        // type ? hash    key?  order_split_amount:[order_id]   field split_amount_sum,origin_amount_sum  value? 累计金额
        val key = "order_split_amount:" + orderWide.order_id
        val orderSumMap: util.Map[String, String] = jedis.hgetAll(key)
        var splitAmountSum = 0D
        var originAmountSum = 0D
        if (orderSumMap != null && orderSumMap.size() > 0) {
          val splitAmountSumString: String = orderSumMap.get("split_amount_sum")
          splitAmountSum = splitAmountSumString.toDouble

          val originAmountSumString: String = orderSumMap.get("origin_amount_sum")
          originAmountSum = originAmountSumString.toDouble
        }
        //  2 先判断是否是最后一笔： （单价*数量） == 原始总金额 - （其他明细【单价*数量】的合计）
        val detailOrginAmount: Double = orderWide.sku_price * orderWide.sku_num
        val restOrginAmount: Double = orderWide.final_total_amount - originAmountSum
        if (detailOrginAmount == restOrginAmount) {
          //  3.1  如果是最后一笔 使用减法：实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
          orderWide.final_detail_amount = orderWide.final_total_amount - splitAmountSum
        } else {
          //  3.2  如果 不是最后一笔：用乘除计算：实付分摊金额=（单价*数量）*实付总金额 / 原始总金额
          orderWide.final_detail_amount = detailOrginAmount * orderWide.final_total_amount / orderWide.original_total_amount
          orderWide.final_detail_amount = Math.round(orderWide.final_detail_amount * 100D) / 100D
        }
        //  4   进行合计保存
        //              hincr
        //              【实付分摊金额】的合计，【单价*数量】的合计
        splitAmountSum += orderWide.final_detail_amount
        originAmountSum += detailOrginAmount
        orderSumMap.put("split_amount_sum", splitAmountSum.toString)
        orderSumMap.put("origin_amount_sum", originAmountSum.toString)
        jedis.hmset(key, orderSumMap)
      }
      jedis.close()
      orderWideList.toIterator
    }

//    orderWideWithSplitDstream.cache()
//    orderWideWithSplitDstream.map(orderwide=>JSON.toJSONString(orderwide,new SerializeConfig(true))).print(1000)


    val orderWideKafkaSentDstream: DStream[OrderDetailWide] = orderWideWithSplitDstream.mapPartitions { orderWideItr =>
      val orderWideList: List[OrderDetailWide] = orderWideItr.toList
      for (orderWide <- orderWideList) {
        XxKafkaSink.send("DWS_ORDER_WIDE", JSON.toJSONString(orderWide, new SerializeConfig(true)))
      }
      orderWideItr.toIterator
    }

    //写入到clickhouse中
    val sparkSession = SparkSession.builder()
      .appName("order_detail_wide_spark_app")
      .getOrCreate()

    import sparkSession.implicits._
    orderWideKafkaSentDstream.foreachRDD{rdd=>
      val df: DataFrame = rdd.toDF()

      df.write.mode(SaveMode.Append)
        .option("batchsize","100")
        .option("isolationLevel","NONE") //设置事务
        .option("numPartitions","4") //设置并发
        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://centos01:8123/test","order_wider_all",new Properties())


      OffsetManager.saveOffset(topicOrderInfo,groupIdOrderInfo,orderInfoOffsetRanges)
      OffsetManager.saveOffset(topicOrderDetail,groupIdOrderDetail,OrderDetailOffsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()

  }
}
