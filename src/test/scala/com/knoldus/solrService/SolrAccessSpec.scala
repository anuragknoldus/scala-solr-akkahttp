package com.knoldus.solrService

import com.knoldus.solrService.factories.{BookDetails, SolrAccess}
import com.typesafe.config.ConfigFactory
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.response.UpdateResponse
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.util.NamedList
import org.scalatest.FunSuite
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar


class SolrAccessSpec extends FunSuite with MockitoSugar {
  val config = ConfigFactory.load("application.conf")
  val url = config.getString("solr.url")
  val collection_name = config.getString("solr.collection")
  val url_final = url + collection_name

  val mockConnection = mock[HttpSolrClient]

  val solrAccess = new SolrAccess(mockConnection, mockConnection)
  val mockSDOC: SolrInputDocument = mock[SolrInputDocument]
  test("insert data") {
    val book_Details = BookDetails("124569-0000-363",
      Array("book", "education", "solr"),
      "Solr",
      "Henry/",
      Some("education"),
      2,
      "education",
      true,
      1253.1,
      2569)

    val uR = new UpdateResponse
    val a: NamedList[AnyRef] = new NamedList[AnyRef](1)
    a.add("responseHeader","{status=0,QTime=408}")
    /*a.add("status", new Integer(0))
    a.add("QTime", new Integer(408))*/
    uR.setResponse(a)
    print(":::::::::::::::::::::: UR " + uR)
    val solrInputDocument = new SolrInputDocument
    solrInputDocument.addField("id", book_Details.id)
    solrInputDocument.addField("cat", book_Details.cat)
    solrInputDocument.addField("name", book_Details.name)
    solrInputDocument.addField("author", book_Details.author)
    solrInputDocument.addField("series_t", book_Details.series_t)
    solrInputDocument.addField("sequence_i", book_Details.sequence_i)
    solrInputDocument.addField("genre_s", book_Details.genre_s)
    solrInputDocument.addField("inStock", book_Details.inStock)
    solrInputDocument.addField("price", book_Details.price)
    solrInputDocument.addField("pages_i", book_Details.pages_i)
    when(mockConnection
      .add("gettingstarted", solrInputDocument)) thenReturn
    (uR)
    val result: Option[Int] = solrAccess.createOrUpdateRecord(book_Details, solrInputDocument)
    println(result)
    assert(result.contains(0))
  }

}

