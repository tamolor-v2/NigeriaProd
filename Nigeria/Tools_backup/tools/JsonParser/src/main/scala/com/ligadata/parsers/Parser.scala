package com.ligadata.parsers

import com.ligadata.dataobject.Feed

trait Parser {
  def parseJson(jsonString: String): Feed
}