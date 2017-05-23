package utils

import com.alibaba.fastjson.{JSON, JSONObject}
import org.jvnet.hk2.internal.ClazzCreator

/**
  * Created by Administrator on 2017/4/12.
  */
object JSONParse {

  /**
    * 判断是否是JSON
    *
    * @param jsonString
    * @return
    */
  def isJSON(jsonString: String): Boolean = {
    if (jsonString == null || "".equals(jsonString)) {
      return false
    }
    try {
      JSON.parseObject(jsonString)
      return true
    } catch {
      case e: com.alibaba.fastjson.JSONException => {
        println(jsonString + " is not JSON Type!")
        return false
      }
    }
  }

  /**
    * 解析value为 String 的JSON
    *
    * @param jsonObj
    * @param string
    * @param key
    * @return
    */
  def jsonParseString(jsonObj: JSONObject, string: String, key: String, jsonOut: JSONObject): Unit = {
    val str = jsonObj.getString(string)

    if (str == null) {
      jsonOut.put(key, "")
    } else {
      jsonOut.put(key, str)
    }
  }

  /**
    * 解析value为 Array 的JSON
    *
    * @param jsonObj
    * @param string
    * @param key
    * @return
    */
  def jsonParseArray(jsonObj: JSONObject, string: String, key: String, jsonOut: JSONObject): Unit = {
    val jsonArray = jsonObj.getJSONArray(string)

    if (jsonArray == null) {
      jsonOut.put(key, "")
    } else {
      jsonOut.put(key, jsonArray.size().toString)
    }
  }
}
