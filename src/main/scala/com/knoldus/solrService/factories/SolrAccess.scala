package com.knoldus.solrService.factories

import com.google.gson.Gson
import com.typesafe.config.ConfigFactory
import org.apache.solr.client.solrj.impl.{HttpSolrClient, XMLResponseParser}
import org.apache.solr.client.solrj.response.{QueryResponse, UpdateResponse}
import org.apache.solr.client.solrj.{SolrQuery, SolrServerException}
import org.apache.solr.common.SolrInputDocument
import org.json4s._
import org.json4s.native.JsonMethods._


/**
 * Created by anurag on 22/2/17.
 */


case class BookDetails(
    id: String,
    cat: Array[String],
    name: String,
    author: String,
    series_t: Option[String],
    sequence_i: Int,
    genre_s: String,
    inStock: Boolean,
    price: Double,
    pages_i: Int)


class SolrAccess(solrClientForInsert: HttpSolrClient,solrClientForExecute: HttpSolrClient) {

  val config = ConfigFactory.load("application.conf")
  val url = config.getString("solr.url")
  val collection_name = config.getString("solr.collection")
  val url_final = url + collection_name

  /**
   * This method take a parameter of Book_Details and then insert data or update data if that is
   * present into solr collection. It match unique key and in our case that is id.
   *
   * @param book_Details
   * @return
   */

  def createOrUpdateRecord(book_Details: BookDetails, sdoc: SolrInputDocument): Option[Int] = {
    try {
      //val solrClient = new HttpSolrClient.Builder(url).build()

      println(":::::::::::::::::::  sdoc")
      sdoc.addField("id", book_Details.id)
      sdoc.addField("cat", book_Details.cat)
      sdoc.addField("name", book_Details.name)
      sdoc.addField("author", book_Details.author)
      sdoc.addField("series_t", book_Details.series_t)
      sdoc.addField("sequence_i", book_Details.sequence_i)
      sdoc.addField("genre_s", book_Details.genre_s)
      sdoc.addField("inStock", book_Details.inStock)
      sdoc.addField("price", book_Details.price)
      sdoc.addField("pages_i", book_Details.pages_i)
      println("::::::::::::::::::: sdoc " + sdoc + s" --- $collection_name")
      println("::::::::::::::::::: before calling the function")
      println(sdoc.getFieldNames.toArray().toList)
      val result: UpdateResponse = solrClientForInsert.add(collection_name, sdoc)
      println("::::::::::::::::::::::::::: result in updated Response " + result)
      Some(result.getStatus)
    } catch {
      case solrServerException: SolrServerException =>
        println("Solr Server Exception : " + solrServerException.getMessage)
        None
    }
  }

  /**
   * This method will return total count of records in your solr
   *
   * @return
   */

  def findAllRecord(): Option[List[BookDetails]] = {
    try {
      val parameter = new SolrQuery()
      parameter.set("qt", "/select")
      parameter.set("indent", "on")
      parameter.set("q", "*:*")
      parameter.set("wt", "json")
      executeQuery(parameter) match {
        case Some(data) => Some(data)
        case None => None
      }
    } catch {
      case solrServerException: SolrServerException =>
        println("Solr Server Exception : " + solrServerException.getMessage)
        None
    }
  }

  /**
   * This method will take a keyword and fetch all the record related to that keyword
   *
   * @param keyword eg: fantasy
   * @return total count of the record
   */

  def findRecordWithKeyword(keyword: String): Option[List[BookDetails]] = {
    try {
      val parameter: SolrQuery = new SolrQuery()
      parameter.set("qt", "/select")
      parameter.set("indent", "on")
      parameter.set("q", s"$keyword")
      parameter.set("wt", "json")
      executeQuery(parameter)
    } catch {
      case solrServerException: SolrServerException =>
        println("Solr Server Exception : " + solrServerException.getMessage)
        None
    }
  }

  /**
   * This method will take a key and value, after this it will fetch all the record related to that
   * key and value. eg: ("name", "scala")
   *
   * @param key   : name
   * @param value : scala
   * @return
   */


  def findRecordWithKeyAndValue(key: String, value: String): Option[List[BookDetails]] = {
    try {
      val keyValue = s"$key:" + s"${ value.trim }"
      val parameter = new SolrQuery()
      parameter.set("qt", "/select")
      parameter.set("indent", "true")
      parameter.set("q", s"$keyValue")
      parameter.set("wt", "json")
      executeQuery(parameter)
    } catch {
      case solrServerException: SolrServerException =>
        println("Solr Server Exception : " + solrServerException.getMessage)
        None
    }
  }

  /**
   * This is a private method which take the value of solrQuery and then execute query with solr
   * client and after execution it parse result into Case Class and create a List[CaseClass].
   *
   * @param parameter : Query
   * @return
   */

  private def executeQuery(parameter: SolrQuery): Option[List[BookDetails]] = {
    try {
      //val solrClient: HttpSolrClient = new HttpSolrClient.Builder(url_final).build()
      //val solrClient = new HttpSolrClient.Builder(url).build()
      solrClientForExecute.setParser(new XMLResponseParser())
      val gson = new Gson()
      val response: QueryResponse = solrClientForExecute.query(parameter)
      implicit val formats = DefaultFormats
      val data: List[BookDetails] = parse(gson.toJson(response.getResults))
        .extract[List[BookDetails]]
      solrClientForExecute.close()
      Some(data)
    } catch {
      case solrServerException: SolrServerException =>
        println("Solr Server Exception : " + solrServerException.getMessage)
        None
    }

  }
}

//object SolrAccess extends SolrAccess