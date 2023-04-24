package dist.group2;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.UnknownHostException;

@SpringBootApplication
public class NamingServerApplication {
	public static void main(String[] args) {
		// Run Naming Server
		SpringApplication.run(NamingServerApplication.class, args);
	}
}