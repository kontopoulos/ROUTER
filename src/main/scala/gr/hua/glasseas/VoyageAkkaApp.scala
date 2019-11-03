package gr.hua.glasseas

import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory
import gr.hua.glasseas.cluster.threads.Ship
import gr.hua.glasseas.cluster.threads.Ship._

object VoyageAkkaApp {

  def main(args: Array[String]): Unit = {

    val system_config = ConfigFactory.parseString(
      """
        |akka {
        |  actor {
        |    provider = "cluster"
        |  }
        |  remote {
        |    netty.tcp {
        |      hostname = "127.0.0.1"
        |      port = 2551
        |    }
        |  }
        |  cluster {
        |    seed-nodes = [
        |      "akka.tcp://GLASSEAS@127.0.0.1:2551"
        |    ]
        |    sharding {
        |      number-of-shards = 10
        |    }
        |  }
        |}
        |""".stripMargin)

    val system = ActorSystem(Behaviors.empty[Ship.Command], "GLASSEAS",system_config)
    val sharding = ClusterSharding(system)

    val shardRegion = sharding.init(Entity(Ship.TypeKey, ctx => defaultThreadBehavior(ctx.entityId)))

//    shardRegion ! ShardingEnvelope("test", End)


    val gc =  new GlasseasContext
    val stream = gc.readStream("test.csv")

    var ids: Set[Int] = Set()
    while (stream.hasNext) {
      val pos = stream.next()
      ids += pos.id
      shardRegion ! ShardingEnvelope(pos.id.toString, aisMessage(pos))
    }
    ids.foreach(id => shardRegion ! ShardingEnvelope(id.toString, End))








  }

}
