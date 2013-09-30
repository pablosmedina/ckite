package the.walrus.ckite.bootstrap.spring

import org.springframework.context.annotation.Configuration
import org.springframework.web.servlet.config.annotation.EnableWebMvc
import org.springframework.context.annotation.ComponentScan
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter

@Configuration @EnableWebMvc @ComponentScan(basePackages = Array("the.walrus"))
class SpringWebConfigurer extends WebMvcConfigurerAdapter