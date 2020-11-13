package com.xiao.newmall.realtime.util

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Index, Search, SearchResult}
import org.elasticsearch.action.search.MultiSearchResponse.Item
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, RangeQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder


object XxEsUtil {
  private    var  factory:  JestClientFactory=null;

  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://centos01:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(1000).build())

  }




  def putIndex(args: Array[String]): Unit = {

    val jest: JestClient = getClient
    val actorList=new util.ArrayList[String]()
    actorList.add("tom")
    actorList.add("Jack")
    val movieTest = MovieTest("102","中途岛之战",actorList)
    val datestring: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
    val index: Index = new Index.Builder(movieTest).index("movie_test_"+datestring).`type`("_doc").id(movieTest.id).build()

    jest.execute(index)
    jest.close()

  }

  //把结构封装的Map 必须使用Java的 不能使用Scala的
  def queryMovie(): Unit ={
    val jest: JestClient = getClient
    var query="{\n  \"query\": {\n    \"bool\": {\n      \"should\": [\n        {\"match\": {\n          \"name\": \"red sea\"\n        }}\n      ],\n      \"filter\": {\n         \"range\": {\n           \"doubanScore\": {\n             \"gte\": 3\n           }\n         }\n      }\n    } \n    \n  }\n  ,\n  \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"asc\"\n      }\n    }\n  ],\n  \"from\": 0, \n  \"size\": 20, \n  \"highlight\": {\n    \"fields\": { \n      \"name\": {}\n    }\n  } \n}";

    val searchBuilder = new SearchSourceBuilder
    val boolbuilder = new BoolQueryBuilder()
    boolbuilder.must(new MatchQueryBuilder("name","red sea"))
    .must(new MatchQueryBuilder("actorList.name","hai"))
    //boolbuilder.should(new MatchQueryBuilder("name","sea"))
      .filter(new RangeQueryBuilder("doubanScore").gte(3))
    searchBuilder.query(boolbuilder)
    searchBuilder.sort("doubanScore",SortOrder.ASC)
    searchBuilder.from(0)
    searchBuilder.size(20)
    searchBuilder.highlight(new HighlightBuilder().field("name"))


    val query2: String = searchBuilder.toString
    println(query2)
    val search: Search = new Search.Builder(query2).addIndex("movie_index").addType("movie").build()
    val result: SearchResult = jest.execute(search)
    val hitList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String,Any]])

    /*    var resultList:ListBuffer[util.Map[String,Any]]=new  ListBuffer[util.Map[String, Any]]
        import  scala.collection.JavaConversions._
        for ( hit <- hitList ) {
          val source: util.Map[String, Any] = hit.source
          resultList.add(source)
        }
        println(resultList.mkString("\n"))*/
    //或者
    import  scala.collection.JavaConversions._
    val list: List[util.Map[String, Any]] = hitList.map(_.source).toList
    println(list.mkString("\n"))

    jest.close()
  }

  def main(args: Array[String]): Unit = {
    //putIndex(args)

    queryMovie
  }

  case class MovieTest(id:String ,movie_name:String, actionNameList: java.util.List[String] ){

  }
}
