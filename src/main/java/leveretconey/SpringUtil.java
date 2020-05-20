package leveretconey;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class SpringUtil {
    public static ApplicationContext context;
    public static void main(String[] args) {
        context = SpringApplication.run(SpringUtil.class, args);
    }
}
