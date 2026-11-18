import java.net.URI;
import java.net.http.*;
import java.time.Duration;
public class Healthcheck {
    public static void main(String[] args) throws Exception {
        String port=System.getenv().getOrDefault("MANAGEMENT_PORT","9080");
        var request=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/actuator/health/readiness")).timeout(Duration.ofSeconds(3)).build();
        int status=HttpClient.newHttpClient().send(request,HttpResponse.BodyHandlers.discarding()).statusCode();
        System.exit(status==200?0:1);
    }
}
