package ckite

import com.typesafe.config.Config

trait ConfigAware {

  implicit def config: Config
}
