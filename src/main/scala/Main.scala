import javassist.bytecode.SignatureAttribute.ArrayType
import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.functions._
import java.sql.PreparedStatement
import java.sql.SQLException
import scala.io.StdIn._
import scala.StringBuilder._
import scala.util.control.Breaks
import scala.util.control.Breaks._
import scala.io.AnsiColor._
import java.awt.event.KeyEvent
import java.awt.Toolkit
import java.sql.ResultSet
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import java.sql.SQLException
import java.sql.BatchUpdateException
import java.sql.DataTruncation
import java.sql.SQLWarning
import ujson._
import scala.language.dynamics._
import com.github.pathikrit.dijon._
import javax.validation.constraints.Digits
//import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source
import scala.io.BufferedSource
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import scala.collection.mutable.Map

//    CREATE TABLES AND PARTITIONS (source info, content, and reading level index; partition by source)
//    spark.sql("SET hive.exec.dynamic.partition = true")
//    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
//    spark.sql("DROP TABLE IF EXISTS readability")
//    spark.sql("CREATE TABLE (source STRING, content STRING, read_lvl) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'")



object Main extends App {

//  JDBC
  val driver = "mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/ProjectZero"
  val username = "root"
  val password = "U-)9tCLmxx4yjyB@!U*!L:>yxeu"

  //  uJSON
val wd = os.pwd/"TEST"

//  NewsData API
  val newsDataKey = "pub_31316e9fe98176be0c8d30380afc458c0316"


  var passwordCorrect = new StringBuilder("...")
  var usersPasswords = Map[String, String]()

//  def logon(): Unit = {
//    var x = new StringBuilder()
//    val logons = List("logon", "LOGON", "exit", "EXIT")
//    breakable {
//      while (!logons.contains(x.toString())) {
//        x.clear()
//        println(s"${BOLD}To logon, type 'logon' and press ENTER. (To log on as administrator, type 'admin' and press ENTER.)")
//        Thread.sleep(200)
//        x.append(readLine(s"To exit, type 'exit' and press ENTER:\n  ${RESET}"))
//        if (logons.contains(x.toString())) break()
//        //        else println(s"${RED}Invalid input.${RESET}")
//        else {
//          println(s"${RED}Invalid input.${RESET}")
//          Thread.sleep(500)
//        }
//      }
//    }
//
//    if (x.toString() == "EXIT" || x.toString() == "exit") exit()
//    else {
//      val isOn = Toolkit.getDefaultToolkit.getLockingKeyState(KeyEvent.VK_CAPS_LOCK)
//      if (isOn) println(s"${RED}Easy there, killer! CAPS LOCK is on.${RESET}")
//      val userCorrect: String = "test"
//      var passwordCorrect = new StringBuilder("...")
//      var userNameInput = new StringBuilder()
//      var passwordInput = new StringBuilder()
//      retrievePasswords()
//
//      //      Enter and validate user name.
//
//      breakable {
//        while (passwordInput.toString != passwordCorrect.toString()) {
//          Thread.sleep(250)
//          userNameInput.clear()
//          breakable {
//            while (!(usersPasswords.contains(userNameInput.toString()))) {
//              passwordCorrect.clear()
//              userNameInput.clear()
//              println()
//              userNameInput.append(readLine(s"${BOLD}Enter your user name, then press ENTER:\n  ${RESET}"))
//              if (usersPasswords.contains(userNameInput.toString())) {
//                passwordCorrect.append(usersPasswords(userNameInput.toString()))
//                break()
//              }
//              else println(s"${RED}There is no user by that name in the system. Please re-enter your user name.${RESET}")
//            }
//
//            //            password checks should go here, I think
//
//          }
//
//          passwordInput.clear()
//          println()
//          passwordInput.append(readLine(s"Enter your password:          (it's ${passwordCorrect}, btw.)\n  "))
//          if (passwordInput.toString() == passwordCorrect.toString()) {
//            Thread.sleep(250)
//            println()
//            println(s"${BLUE}${BOLD}Welcome!${RESET}")
//            Thread.sleep(300)
//            break()
//          }
//
//          else {
//            println(s"${RED}Incorrect user name or password. Passwords are case-sensitive.${RESET}")
//            Thread.sleep(250)
//          }
//        }
//      }
//    }
//  }
//  def retrievePasswords(): Unit = {
//    var connection: Connection = DriverManager.getConnection(url, username, password)
//    val statement = connection.createStatement()
//    var resultSet = statement.executeQuery(s"SELECT userName, password FROM Passwords")
//    var i = 0
//    while (resultSet.next()) {
//      //            println(resultSet.getString(1)+", " +resultSet.getString(2))
//      usersPasswords += (resultSet.getString("userName") -> resultSet.getString("password"))
//      i += 1
//    }
//    println(usersPasswords)
//    connection.close()
//  }
//  def menu(): Unit = {
////    println(BOLD)
////    for (c <- "Loading...") {
////      Thread.sleep(250)
////      print(" " + c)
////    }
////    println(RESET)
//    println()
//    Thread.sleep(900)
//    val options = List("1", "2", "3", "4", "5", "6", "7","8","9")
//    var y = new StringBuilder
//    breakable {
//      while (!options.contains(y.toString())) {
//        y.clear()
//        println(s"${BOLD}Please select from the following options:${RESET}")
//        Thread.sleep(500)
//        println("1. Check current balance\n2. View transactions (last 30 days)\n3. View transactions (last 60 days)\n4. View transactions (last 90 days)\n5. Spending report (30 days)\n6. Spending report (60 days)\n7. Spending report (90 days)\n8. View spending goals\n9. Log off\n  ")
//        y.append(readLine())
//        y.toString() match {
//          case "1" => Menu1(); break();
//          case "2" => Menu2(); break();
//          case "3" => Menu3(); break();
//          case "4" => Menu4(); break();
//          case "5" => Menu5(); break();
//          case "6" => Menu6(); break();
//          case "7" => changePassword(); break();
//          case "8" => Menu8(); break()
//          case "9" => Thread.sleep(250); exit(); break()
//          case _ => println(s"${RED}Invalid input.${RESET}"); Thread.sleep(500);
//        }
//      }
//    }
//  }
//  def Menu1(): Unit = {
//    System.setProperty("hadoop.home.dir", "C:\\hadoop")
//    val spark = SparkSession
//      .builder
//      .appName("hello hive")
//      .config("spark.master", "local")
//      .enableHiveSupport()
//      .getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")
////    spark.sql("SET hive.exec.dynamic.partition = true")
////    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
////    spark.sql("DROP table IF EXISTS BevC")
////    spark.sql("create table IF NOT EXISTS NewBevC(Beverage String, BranchID String) row format delimited fields terminated by ',' stored as textfile")
////    spark.sql("LOAD DATA LOCAL INPATH 'Bev_BranchC.txt' INTO TABLE NewBevC")
////    spark.sql("SELECT * FROM NewBevC").show()
//    spark.sql("SET hive.exec.dynamic.partition = true")
//    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
//    spark.sql("DROP TABLE IF EXISTS readability")
//    spark.sql("CREATE TABLE (source STRING, content STRING, read_lvl) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'")
//    spark.close()
//  }
//  def Menu2(): Unit = {
//
//    val filename = "C:\\Scala Programs 3.0\\untitled\\TEST\\CLI Samples.txt"
//    for (line <- Source.fromFile(filename).getLines) {
//      println(line)
//    }
//
//    menu()
//  }

  def MenuMaster(): Unit = {

//    INITIATING SPARK CONTEXT.
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._

//    spark.sql("CREATE TABLE IF NOT EXISTS storedTables (tables STRING)")
//    spark.sql("CREATE TABLE IF NOT EXISTS Passwords (userName STRING, password STRING)")
//    spark.sql("INSERT OVERWRITE Passwords VALUES ('bradyd123','1234'),('veronicaflor456','2345')('chaparritafea23','3456')")
//    spark.sql("SELECT * FROM Passwords").show()

//    val passwordsDF = spark.read.csv("C:\\Scala Programs 3.0\\untitled\\TEST\\passwords.csv").toDF("userName","password")
//    passwordsDF.show()
//    val users = passwordsDF.select($"userName").map(f=>f.getString(0)).collect.toArray
//    val passwords = passwordsDF.select($"password").map(f=>f.getString(0)).collect.toArray
//    val userPasswords = users.zip(passwords)

    val usersPasswords = collection.mutable.Map[String,String]("bradyd123"->"1234","veronicaflor456"->"2345","chaparritafea23"->"3456")


    //    REQUEST AND JSON CREATION

//    val res1 = requests.get(s"https://newsapi.org/v2/everything?page=1&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes1(justArticles).json", ujson.read(res1.text)("articles").toString())
//    val res2 = requests.get(s"https://newsapi.org/v2/everything?page=2&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes2(justArticles).json", ujson.read(res2.text)("articles").toString())
//    val res3 = requests.get(s"https://newsapi.org/v2/everything?page=3&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes3(justArticles).json", ujson.read(res3.text)("articles").toString())
//    val res4 = requests.get(s"https://newsapi.org/v2/everything?page=4&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes4(justArticles).json", ujson.read(res4.text)("articles").toString())
//    val res5 = requests.get(s"https://newsapi.org/v2/everything?page=5&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes5(justArticles).json", ujson.read(res5.text)("articles").toString())
////

//    20 API REQUESTS AND JSON WRITES.
//    val res1 = requests.get("https://newsapi.org/v2/everything?page=1&from=2021-12-7&to=2021-12-11&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes1(justArticles).json", ujson.read(res1.text)("articles").toString())
//    val res2 = requests.get("https://newsapi.org/v2/everything?page=2&from=2021-12-7&to=2021-12-11&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes2(justArticles).json", ujson.read(res2.text)("articles").toString())
//    val res3 = requests.get("https://newsapi.org/v2/everything?page=3&from=2021-12-7&to=2021-12-11&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes3(justArticles).json", ujson.read(res3.text)("articles").toString())
//    val res4 = requests.get("https://newsapi.org/v2/everything?page=4&from=2021-12-7&to=2021-12-11&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes4(justArticles).json", ujson.read(res4.text)("articles").toString())
//    val res5 = requests.get("https://newsapi.org/v2/everything?page=5&from=2021-12-7&to=2021-12-11&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes5(justArticles).json", ujson.read(res5.text)("articles").toString())
//
//    val res6 = requests.get("https://newsapi.org/v2/everything?page=1&from=2021-12-12&to=2021-12-18&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes6(justArticles).json", ujson.read(res6.text)("articles").toString())
//    val res7 = requests.get("https://newsapi.org/v2/everything?page=2&from=2021-12-12&to=2021-12-18&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes7(justArticles).json", ujson.read(res7.text)("articles").toString())
//    val res8 = requests.get("https://newsapi.org/v2/everything?page=3&from=2021-12-12&to=2021-12-18&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes8(justArticles).json", ujson.read(res8.text)("articles").toString())
//    val res9 = requests.get("https://newsapi.org/v2/everything?page=4&from=2021-12-12&to=2021-12-18&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes9(justArticles).json", ujson.read(res9.text)("articles").toString())
//    val res10 = requests.get("https://newsapi.org/v2/everything?page=5&from=2021-12-12&to=2021-12-18&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes10(justArticles).json", ujson.read(res10.text)("articles").toString())
//
//    val res11 = requests.get("https://newsapi.org/v2/everything?page=1&from=2021-12-19&to=2021-12-25&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes11(justArticles).json", ujson.read(res11.text)("articles").toString())
//    val res12 = requests.get("https://newsapi.org/v2/everything?page=2&from=2021-12-19&to=2021-12-25&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes12(justArticles).json", ujson.read(res12.text)("articles").toString())
//    val res13 = requests.get("https://newsapi.org/v2/everything?page=3&from=2021-12-19&to=2021-12-25&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes13(justArticles).json", ujson.read(res13.text)("articles").toString())
//    val res14 = requests.get("https://newsapi.org/v2/everything?page=4&from=2021-12-19&to=2021-12-25&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes14(justArticles).json", ujson.read(res14.text)("articles").toString())
//    val res15 = requests.get("https://newsapi.org/v2/everything?page=5&from=2021-12-19&to=2021-12-25&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes15(justArticles).json", ujson.read(res15.text)("articles").toString())
//
//    val res16 = requests.get("https://newsapi.org/v2/everything?page=1&from=2021-12-26&to=2022-01-02&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes16(justArticles).json", ujson.read(res16.text)("articles").toString())
//    val res17 = requests.get("https://newsapi.org/v2/everything?page=2&from=2021-12-26&to=2022-01-02&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes17(justArticles).json", ujson.read(res17.text)("articles").toString())
//    val res18 = requests.get("https://newsapi.org/v2/everything?page=3&from=2021-12-26&to=2022-01-02&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes18(justArticles).json", ujson.read(res18.text)("articles").toString())
//    val res19 = requests.get("https://newsapi.org/v2/everything?page=4&from=2021-12-26&to=2022-01-02&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes19(justArticles).json", ujson.read(res19.text)("articles").toString())
//    val res20 = requests.get("https://newsapi.org/v2/everything?page=5&from=2021-12-26&to=2022-01-02&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015")
//    os.write.over(wd/"newsapiRes20(justArticles).json", ujson.read(res20.text)("articles").toString())
//
//



    //    Creating the news query DF with four columns.
    var df0 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes1(justArticles).json").toDF()
    var df_2 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes2(justArticles).json").toDF()
    var df_3 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes3(justArticles).json").toDF()
    var df_4 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes4(justArticles).json").toDF()
    var df_5 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes5(justArticles).json").toDF()

//    15 ADDITIONAL DATAFRAME CREATIONS.
//    var df_6 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes6(justArticles).json").toDF()
//    var df_7 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes7(justArticles).json").toDF()
//    var df_8 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes8(justArticles).json").toDF()
//    var df_9 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes9(justArticles).json").toDF()
//    var df_10 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes10(justArticles).json").toDF()
//    var df_11 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes11(justArticles).json").toDF()
//    var df_12 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes12(justArticles).json").toDF()
//    var df_13 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes13(justArticles).json").toDF()
//    var df_14 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes14(justArticles).json").toDF()
//    var df_15 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes15(justArticles).json").toDF()
//    var df_16 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes16(justArticles).json").toDF()
//    var df_17 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes17(justArticles).json").toDF()
//    var df_18 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes18(justArticles).json").toDF()
//    var df_19 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes19(justArticles).json").toDF()
//    var df_20 = spark.read.json("C:\\Scala Programs 3.0\\untitled\\TEST\\newsapiRes20(justArticles).json").toDF()

    val df1 = df0.union(df_2).union(df_3).union(df_4).union(df_5)
    val df2 = df1.select("source", "content").toDF("Source","Content")
    val totCharCount = udf((x: String) => x.substring(x.lastIndexOf("[+"),x.length).filter(_.isDigit))
//    val visits = udf((x:String) => nuMap("ab"))
    val contentSentences = df2.select($"Source", $"Content",
  split(col("Content"),"\\. ").getItem(0).as("s0"),
  split(col("Content"),"\\. ").getItem(1).as("s1"),
  split(col("Content"),"\\. ").getItem(2).as("s2")
)
  .withColumn("sCt",size((split(col("Content"),"\\. "))))
  .withColumn("wordCount", when($"content".isNotNull,size(split(col("Content"), " "))).otherwise(null))
  .withColumn("charCount",length($"Content"))
//      The following column should behave as an Int. I successfully added another integer to it.
  .withColumn("totCharCount",totCharCount($"Content").cast("Double"))
  .withColumn("words0",size(split($"s0"," ")))
  .withColumn("words1",when($"s1".isNull, null).otherwise(size(split($"s1"," "))))
  .withColumn("words2",when($"s2".isNull, null).otherwise(size(split($"s2"," "))))
  .withColumn("CLI",$"charCount"/$"wordCount"*100*0.0588 - $"sCt"/$"wordCount"*100*4.20 - 15.8)
  .withColumn("srcVisits30d", when($"Source".cast("String").contains("[nbc-news, NBC News]"),72.1).when($"Source".cast("String").contains("[, New York Post]"),120.3).when($"Source".cast("String").contains("[, Los Angeles Times]"),37.9).when($"Source".cast("String").contains("[, New York Daily News]"),11.8).when($"Source".cast("String").contains("[, The Guardian]"),276.9).when($"Source".cast("String").contains("[, CNET]"),71.7).when($"Source".cast("String").contains("[, Daily Mail]"),307.5).when($"Source".cast("String").contains("[fox-news, Fox News]"),302.5).when($"Source".cast("String").contains("[usa-today, USA Today]"),107.8).when($"Source".cast("String").contains("[, SFGate]"),24.6).when($"Source".cast("String").contains("[independent, Independent]"),88.4).when($"Source".cast("String").contains("[, The Boston Globe]"),9).otherwise(null))
  .withColumn("srcBounce", when($"Source".cast("String").contains("[nbc-news, NBC News]"),0.75).when($"Source".cast("String").contains("[, New York Post]"),0.67).when($"Source".cast("String").contains("[, Los Angeles Times]"),0.74).when($"Source".cast("String").contains("[, New York Daily News]"),0.66).when($"Source".cast("String").contains("[, The Guardian]"),0.58).when($"Source".cast("String").contains("[, CNET]"),0.65).when($"Source".cast("String").contains("[, Daily Mail]"),0.62).when($"Source".cast("String").contains("[fox-news, Fox News]"),0.47).when($"Source".cast("String").contains("[usa-today, USA Today]"),0.70).when($"Source".cast("String").contains("[, SFGate]"),0.63).when($"Source".cast("String").contains("[independent, Independent]"),0.72).when($"Source".cast("String").contains("[, The Boston Globe]"),0.59).otherwise(null))
  .withColumn("srcPPV", when($"Source".cast("String").contains("[nbc-news, NBC News]"),1.45).when($"Source".cast("String").contains("[, New York Post]"),1.84).when($"Source".cast("String").contains("[, Los Angeles Times]"),1.52).when($"Source".cast("String").contains("[, New York Daily News]"),1.84).when($"Source".cast("String").contains("[, The Guardian]"),2.56).when($"Source".cast("String").contains("[, CNET]"),2.29).when($"Source".cast("String").contains("[, Daily Mail]"),2.89).when($"Source".cast("String").contains("[fox-news, Fox News]"),3.12).when($"Source".cast("String").contains("[usa-today, USA Today]"),2.19).when($"Source".cast("String").contains("[, SFGate]"),2.3).when($"Source".cast("String").contains("[independent, Independent]"),1.83).when($"Source".cast("String").contains("[, The Boston Globe]"),3.03).otherwise(null))
  .withColumn("srcAvgDur", when($"Source".cast("String").contains("[nbc-news, NBC News]"),71.toDouble).when($"Source".cast("String").contains("[, New York Post]"),141.toDouble).when($"Source".cast("String").contains("[, Los Angeles Times]"),75.toDouble).when($"Source".cast("String").contains("[, New York Daily News]"),67.toDouble).when($"Source".cast("String").contains("[, The Guardian]"),220.toDouble).when($"Source".cast("String").contains("[, CNET]"),93.toDouble).when($"Source".cast("String").contains("[, Daily Mail]"),262.toDouble).when($"Source".cast("String").contains("[fox-news, Fox News]"),467.toDouble).when($"Source".cast("String").contains("[usa-today, USA Today]"),269.toDouble).when($"Source".cast("String").contains("[, SFGate]"),320.toDouble).when($"Source".cast("String").contains("[independent, Independent]"),109.toDouble).when($"Source".cast("String").contains("[, The Boston Globe]"),615.toDouble).otherwise(null))
  .filter($"CLI" > 0)
  .toDF()
//

contentSentences.createOrReplaceTempView("contentSentences")

//println("NEWTHING: ")
//    val NEWTHING = contentSentences.join(trafficStats,contentSentences("source")===trafficStats("_c0")).show()

//  spark.sql("DROP TABLE IF EXISTS df2Bucketed")
////    dbutils.fs.rm("dbfs:/user/hive/warehouse/SomeData/", true)
//  contentSentences.write.bucketBy(3, "CLI").saveAsTable("df2Bucketed")

// Only to display results in buckets, ordered by CLI.
  val newDFa = spark.sql("WITH one AS " +
    "(SELECT " +
    "ROW_NUMBER() OVER (ORDER BY CLI) AS rowNum, Source," +
    "totCharCount, CLI, srcVisits30d, srcBounce, srcPPV, srcAvgDur " +
    "FROM contentSentences) " +
    "SELECT * FROM one WHERE rowNum BETWEEN 1 AND 20")
    newDFa.cache()
  val newDFb = spark.sql("WITH one AS " +
      "(SELECT " +
      "ROW_NUMBER() OVER (ORDER BY CLI) AS rowNum, Source, " +
      "totCharCount, CLI srcVisits30d, srcBounce, srcPPV, srcAvgDur " +
      "FROM contentSentences) " +
      "SELECT * FROM one WHERE rowNum BETWEEN 21 AND 40").toDF()
    newDFb.cache()
  val newDFc = spark.sql("WITH one AS " +
      "(SELECT " +
      "ROW_NUMBER() OVER (ORDER BY CLI) AS rowNum, Source, " +
      "totCharCount, CLI srcVisits30d, srcBounce, srcPPV, srcAvgDur " +
      "FROM contentSentences) " +
      "SELECT * FROM one WHERE rowNUM > 40").toDF()
    newDFc.cache()

//    Summary data for each source, including average CLI.
    val sourceSummaryDF1 = contentSentences.groupBy("source").avg("CLI").toDF()
    sourceSummaryDF1.cache()
    val sourceSummaryDF2 = contentSentences.select("Source","srcVisits30d","srcBounce","srcPPV","srcAvgDur").toDF()
    val sourceSummaryDF3 = sourceSummaryDF1.join(sourceSummaryDF2,sourceSummaryDF1("Source") === sourceSummaryDF2("Source")).toDF()
    sourceSummaryDF3.createOrReplaceTempView("sourceSummaryDF3")

    //    "Independent" variables: CLI, article length (totCharCount).
    val cliArr = contentSentences.select($"CLI").map(f=>f.getDouble(0)).collect.toArray
    val totCharsArr = contentSentences.select("totCharCount").map(f=>f.getDouble(0)).collect.toArray

//    "Dependent" variables: visits last month, bounce rate, avg pg per visit, avg duration.
    val srcVisitsArr = sourceSummaryDF3.select($"srcVisits30d").map(f=>f.getDouble(0)).collect.toArray
    val srcBounceArr = sourceSummaryDF3.select($"srcBounce").map(f=>f.getDouble(0)).collect.toArray
    val srcPPVArr = sourceSummaryDF3.select($"srcPPV").map(f=>f.getDouble(0)).collect.toArray
    val srcAvgDurArr = sourceSummaryDF3.select($"srcAvgDur").map(f=>f.getDouble(0)).collect.toArray

    val pearson1 = new PearsonsCorrelation().correlation(cliArr,srcVisitsArr)
    val pearson2 = new PearsonsCorrelation().correlation(cliArr,srcBounceArr)
    val pearson3 = new PearsonsCorrelation().correlation(cliArr,srcPPVArr)
    val pearson4 = new PearsonsCorrelation().correlation(cliArr,srcAvgDurArr)

    val pearson5 = new PearsonsCorrelation().correlation(totCharsArr,srcVisitsArr)
    val pearson6 = new PearsonsCorrelation().correlation(totCharsArr,srcBounceArr)
    val pearson7 = new PearsonsCorrelation().correlation(totCharsArr,srcPPVArr)
    val pearson8 = new PearsonsCorrelation().correlation(totCharsArr,srcAvgDurArr)

    val correlations = spark.read.csv("C:\\Scala Programs 3.0\\untitled\\TEST\\Correlations.csv").toDF().select("_c0","_c1","_c2","_c3").toDF("CLI/bounceRate","CLI/visitsLastMonth","CLI/pagesPerVisit","CLI/avgDuration")
//    correlations.show()





    def logon(): Unit = {
      println("logon")
//      val driver = "mysql.cj.jdbc.Driver"
//      val url = "jdbc:mysql://localhost:3306/ProjectZero"
//      val username = "root"

      var x = new StringBuilder()
      val logons = List("logon", "LOGON", "admin", "ADMIN","exit", "EXIT")
      breakable {
        while (!logons.contains(x.toString())) {
          x.clear()
//          println(s"${BOLD}To logon, type 'logon' and press ENTER. (To log on as administrator, type 'admin' and press ENTER.)")
          println(s"${BOLD}To logon, type 'logon' and press ENTER.")
          Thread.sleep(200)
          x.append(readLine(s"To exit, type 'exit' and press ENTER:\n  ${RESET}"))
          if (logons.contains(x.toString())) break()
          //        else println(s"${RED}Invalid input.${RESET}")
          else {
            println(s"${RED}Invalid input.${RESET}")
            Thread.sleep(500)
          }
        }
      }

      if (x.toString() == "EXIT" || x.toString() == "exit") exit()
      else {
        val isOn = Toolkit.getDefaultToolkit.getLockingKeyState(KeyEvent.VK_CAPS_LOCK)
        if (isOn) println(s"${RED}Easy there, killer! CAPS LOCK is on.${RESET}")
        val userCorrect: String = "test"
        var passwordCorrect = new StringBuilder("...")
        var userNameInput = new StringBuilder()
        var passwordInput = new StringBuilder()
        retrievePasswords()

        //      Enter and validate user name.
        breakable {
          while (passwordInput.toString != passwordCorrect.toString()) {
            Thread.sleep(250)
            userNameInput.clear()
            breakable {
              while (!(usersPasswords.contains(userNameInput.toString()))) {
                passwordCorrect.clear()
                userNameInput.clear()
                println()
                userNameInput.append(readLine(s"${BOLD}Enter your user name, then press ENTER:\n  ${RESET}"))
                if (usersPasswords.contains(userNameInput.toString())) {
                  passwordCorrect.append(usersPasswords(userNameInput.toString()))
                  break()
                }
                else println(s"${RED}There is no user by that name in the system. Please re-enter your user name.${RESET}")
              }

            }

            passwordInput.clear()
            println()
            passwordInput.append(readLine(s"Enter your password:          (it's ${passwordCorrect}, btw.)\n  "))
            if (passwordInput.toString() == passwordCorrect.toString()) {
              Thread.sleep(250)
              println()
              println(s"${BLUE}${BOLD}Welcome!${RESET}")
              Thread.sleep(300)
              break()
            }

            else {
              println(s"${RED}Incorrect user name or password. Passwords are case-sensitive.${RESET}")
              Thread.sleep(250)
            }
          }
        }
      }
    }
    def retrievePasswords(): Unit = {
//      var connection: Connection = DriverManager.getConnection(url, username, password)
//      val statement = connection.createStatement()
//      var resultSet = statement.executeQuery(s"SELECT userName, password FROM Passwords")
//      var i = 0
//      while (resultSet.next()) {
//        //            println(resultSet.getString(1)+", " +resultSet.getString(2))
//        usersPasswords += (resultSet.getString("userName") -> resultSet.getString("password"))
//        i += 1
//      }
//      println(usersPasswords)
//      connection.close()


      println

    }

//    Main menu.
    def menu(): Unit = {
      //    println(BOLD)
      //    for (c <- "Loading...") {
      //      Thread.sleep(250)
      //      print(" " + c)
      //    }
      //    println(RESET)
      println()
      Thread.sleep(900)
      val options = List("1", "2", "3", "4", "5", "6", "7","8","9")
      var y = new StringBuilder
      breakable {
        while (!options.contains(y.toString())) {
          y.clear()
          println(s"${BOLD}Please select from the following options:${RESET}")
          Thread.sleep(500)
          println("1. Display CLI samples. \n2. Display definitions. \n3. Display article information (readability scores, length, etc.). \n4. Display source information (avg CLIs and traffic data) \n5. Display Pearson's R coefficients. \n6. Change password. \n7. Exit \n")
          y.append(readLine())
          y.toString() match {
            case "1" => Menu1(); break();
            case "2" => Menu2(); break();
            case "3" => Menu3(); break();
            case "4" => Menu4(); break();
            case "5" => Menu5(); break();
            case "6" => changePassword(); break();
            case "7" => Thread.sleep(250); exit(); break()
            case _ => println(s"${RED}Invalid input.${RESET}"); Thread.sleep(500);
          }
        }
      }
    }

//    Displays CLI samples.
    def Menu1(): Unit = {
      val filename = "C:\\Scala Programs 3.0\\untitled\\TEST\\CLI Samples.txt"
      for (line <- Source.fromFile(filename).getLines) {
        println(line)
      }
      menu()
    }

//    Displays definitions.
    def Menu2(): Unit = {
      val filename = "C:\\Scala Programs 3.0\\untitled\\TEST\\Definitions.txt"
      for (line <- Source.fromFile(filename).getLines) {
        println(line)
      }
      menu()
    }

//    Displays article information (readability scores, length, etc.).
    def Menu3(): Unit = {
          newDFa.show()
          newDFb.show()
          newDFc.show()

      val read = readLine("Save to bucketed table? Press y for yes, or press any other key to continue: ")
      read match {
        case "y" => {
          val name = readLine("Enter a name for the table: ")
          spark.sql(s"DROP TABLE IF EXISTS $name")
          contentSentences.write.bucketBy(3, "CLI").saveAsTable(s"$name")
          println("Table created successfully.")
        }
        case _ =>
      }


      menu()
    }

//    Displays source information, including avg CLIs and traffic data.
    def Menu4(): Unit = {
//      def saveTable():Unit = {
//        val tableName = readLine("Enter a table name and press ENTER: \n")
//        spark.sql(s"DROP TABLE IF EXISTS $tableName")
//        spark.sql(s"CREATE TABLE $tableName AS SELECT * FROM sourceSummaryDF3")
//        println("Table saved!")
//        spark.sql(s"INSERT INTO storedTables VALUES($tableName)")
//        Thread.sleep(2000)
//        menu()
//      }
      sourceSummaryDF3.show()
//      var save = readLine("Save table? Type y for yes, or type any other key to continue. \n")
//      save.toString match {
//        case "y" => saveTable()
//        case _ => println("Menu")
//      }
      menu()
    }

//    Displays Pearson's R coefficients.
    def Menu5(): Unit = {
      correlations.show()
      menu()
    }

    def changePassword(): Unit = {
      var userNameInput = new StringBuilder()
      breakable {
//        while (passwordInput.toString != passwordCorrect.toString()) {
          Thread.sleep(250)
          breakable {
            while (!(usersPasswords.contains(userNameInput.toString()))) {
              userNameInput.clear()
              passwordCorrect.clear()
              userNameInput.clear()
              println()
              userNameInput.append(readLine(s"${BOLD}Enter your user name, then press ENTER:\n  ${RESET}"))
              if (usersPasswords.contains(userNameInput.toString())) {
                println("that user name is in the system! nice!")
                val newPassword = readLine("Enter a new password: \n")
                usersPasswords(s"$userNameInput") = newPassword
                println("Password updated successfully.")
                break()
              }
              else println(s"${RED}There is no user by that name in the system. Please re-enter your user name.${RESET}")
            }
          }
//        }
      }
      menu()
    }

    def exit(): Unit = {
      val delete = readLine("Would you like to delete your tables before exiting? Type y for yes or press any other key to continue: ")
      delete match {
        case "y" => {
          val name = readLine("Enter the name of the table you wish to delete: ")
          spark.sql(s"DROP TABLE IF EXISTS $name")
          println("Table deleted successfully.")
        }
        case _ =>
      }
      println("Goodbye!")
      System.exit(0)
    }

    logon()
    menu()
    spark.close()

  }



//  def Menu4(): Unit = {
//    System.setProperty("hadoop.home.dir", "C:\\hadoop")
//    val spark = SparkSession
//      .builder
//      .appName("hello hive")
//      .config("spark.master", "local")
//      .enableHiveSupport()
//      .getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")
//    spark.sql("CREATE TABLE IF NOT EXISTS jsonTest1")
//    spark.close()
//  }
//  def Menu5(): Unit = {
//    System.setProperty("hadoop.home.dir", "C:\\hadoop")
//    val spark = SparkSession
//      .builder
//      .appName("hello hive")
//      .config("spark.master", "local")
//      .enableHiveSupport()
//      .getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")
//    spark.sql("SET hive.exec.dynamic.partition = true")
//    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
//    spark.sql("DROP table IF EXISTS BevC")
//    spark.sql("create table IF NOT EXISTS BevC(Beverage String, BranchID String) row format delimited fields terminated by ',' stored as textfile")
//    spark.sql("LOAD DATA LOCAL INPATH 'Bev_BranchC.txt' INTO TABLE BevC")
//    spark.sql("SELECT * FROM BevC").show()
//    spark.close()
//  }
//  def Menu6(): Unit = {
//
//    val x = Array[Double](8.152,7.4866666667,16.4430769231,10.3975,7.4866666667,10.21375,8.152,5.011,8.152,6.8572972973,5.6953846154,6.2610526316,6.2610526316,6.6983783784,10.3975)
//    val y = Array[Double](8,10,23,13,10,14,8,7,11,8,5,4,9,10,13)
//    val pearson2 = new PearsonsCorrelation().correlation(y,x)
//    println(pearson2)
//
//  }
//  def changePassword(): Unit = {}
//  def Menu8(): Unit = {}
//  def exit(): Unit = {
//    println("Goodbye!")
//    System.exit(0)
//  }



  MenuMaster()

}






//TRASH HEAP

//val x = Array[Double](8.152,7.4866666667,16.4430769231,10.3975,7.4866666667,10.21375,8.152,5.011,8.152,6.8572972973,5.6953846154,6.2610526316,6.2610526316,6.6983783784,10.3975)
//val y = Array[Double](8,10,23,13,10,14,8,7,11,8,5,4,9,10,13)
//val pearson2 = new PearsonsCorrelation().correlation(y,x)
//println(pearson2)


//    MAKES 10 SUCCESSIVE REQUESTS, ADDING EACH TO THE STRINGBUILDER AND FORMATTING APPROPRIATELY.
//    for ( i <- 2 to 3) {
//      println("FOR LOOP")
//      val string = s"https://newsapi.org/v2/everything?page=\\${i}&domains=nytimes.com,cnn.com,nbcnews.com,huffingtonpost.com,time.com,nypost.com,latimes.com,nydailynews.com,npr.com,msn.com,theguardian.com,cnet.com,bbc.com,elitedaily.com,businessinsider.com,bleachreport.com,washingtonpost.com,dailymail.co.uk,foxnews.com,buzzfeed.com,usatoday.com,cbsnews.com,huffingtonpost.com,nbcnews.com,abcnews.go.com,mashable.com,sfgate.com,slate.com,upworthy.com,theblaze.com,telegraph.co.uk,usnews.com,vice.com,chron.com,gawker.com,examiner.com,vox.com,chicagotribune.com,thedailybeast.com,salon.com,mic.com,mirror.co.uk/news,nj.com,independent.co.uk,freep.com,bostonglobe.com,theatlantic.com,mlive.com,engadget.com,techcrunch.com,boston.com,al.com,dallasnews.com&language=en&apiKey=5fcd3c9b0f06460fbfb08e95b82e9015"
//      val res2 = requests.get(string)
//      var rawText2 = new StringBuilder()
////      rawText2.append(ujson.read(res2.text)("articles"))
//      rawText2.append(ujson.read(res2.text))
//      rawText2=rawText2.slice(1,rawText2.length-1)
//      rawText.append("," + rawText2)
//    }


//spark.sql("DROP TABLE IF EXISTS numbered")
//spark.sql("CREATE TABLE numbered AS (SELECT ROW_NUMBER() OVER (ORDER BY CLI) AS rowNum, Source, Content, s0, s1, s2, sCount, wordCount, charCount, words0, words1, words2, CLI FROM df2Bucketed)").show()
//spark.sql("SELECT * FROM numbered").show()


//
//spark.sql("DROP TABLE IF EXISTS content_sentences2")
//spark.sql("CREATE TABLE content_sentences2 AS SELECT * FROM content_sentences").show()
//spark.sql("SELECT Source, Content, s0, s1, s2, sCount, wordCount, charCount, words0, words1, words2, CLI FROM content_sentences2 WHERE sCount > 1 AND words0 > 2").show()
//

//    val sqlContentSentences2 = spark.sql("SELECT Source, COUNT(*) FROM content_sentences2 WHERE sCount > 1 AND words0 > 2 GROUP BY Source").show()
//    val sqlContentSentences3 = spark.sql("SELECT COUNT(*) FROM content_sentences2").show()
//  val sqlContentSentences = spark.sql("WITH one AS (SELECT * FROM content_sentences WHERE sCount > 1 AND words0 > 2) SELECT * FROM one").show()
