package gr.hua.glasseas

case class AISPosition(id: Int, imo: Int, latitude: Double, longitude: Double,
                       cog: Double, heading: Double, speed: Double, seconds: Long, timestamp: String,
                       shipname: String, shipType: String, destination: String, annotation: String) {

  override def toString: String = s"$id,$imo,$latitude,$longitude,$cog,$heading,${speed*10.0},$timestamp,$shipname,$shipType,$destination,$annotation"

  override def hashCode(): Int = id

}
