import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

// load map data
val myMapText = sc.parallelize(
  IOUtils.toString(
      new URL("https://gist.githubusercontent.com/Madhuka/74cb9a6577c87aa7d2fd/raw/2f758d33d28ddc01c162293ad45dc16be2806a6b/data.csv?raw=true"),
      Charset.forName("utf8")).split("\n"))


case class Map(Country:String, Name:String, lat : Float, lan : Float, Altitude : Float)

val myMap = myMapText.map(s=>s.split(",")).filter(s=>s(0)!="Country").map(
     s=>Map(s(0),
             s(1),
             s(2).toFloat,
             s(3).toFloat,
             s(4).toFloat
         )


  myMap.registerTempTable("myMap")
