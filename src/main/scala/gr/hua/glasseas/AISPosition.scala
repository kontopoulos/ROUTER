package gr.hua.glasseas

case class AISPosition(mmsi: Int, imo: Int, latitude: Double, longitude: Double,
                       cog: Double, heading: Double, speed: Double, seconds: Long, timestamp: String,
                       shipname: String, shipType: String, destination: String) {

  override def toString: String = s"$mmsi,$imo,$latitude,$longitude,$cog,$heading,${speed*10.0},$timestamp,$shipname,$shipType,$destination"

  override def hashCode(): Int = mmsi

}
