package com.sneaksanddata.arcane.framework
package models

import com.sneaksanddata.arcane.framework.models.cdm.SimpleCdmModel
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should

import scala.io.Source
import scala.util.Using
import upickle.default.*


class SimpleCdmModelTests extends AnyFlatSpec with Matchers {
  it should "deserialize the model.json file correctly" in {
    val serialized = Using(Source.fromURL(getClass.getResource("/cdm_model.json"))) {
      _.getLines().mkString("\n")
    }.get

    val result = read[SimpleCdmModel](serialized)

    (result.entities.size should be(3), result.entities.head.attributes.size should be (26), result.entities(1).attributes.size should be(12), result.entities(2).attributes.size should be(16))
  }

}
