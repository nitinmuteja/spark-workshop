package template.spark

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}

trait TestWrapper extends FlatSpec
  with Matchers
  with ScalaFutures
  with BeforeAndAfterAll
  with GivenWhenThen
