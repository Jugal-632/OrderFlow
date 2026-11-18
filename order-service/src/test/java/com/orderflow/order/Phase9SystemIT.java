package com.orderflow.order;

import com.fasterxml.jackson.databind.*;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfSystemProperty(named="phase9.system",matches="true")
class Phase9SystemIT {
    @Container static PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:15.6");
    @Container static KafkaContainer broker=new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
    @Container static GenericContainer<?> redis=new GenericContainer<>("redis:7.2.4-alpine").withExposedPorts(6379);
    final Path root=Path.of("..").toAbsolutePath().normalize(),evidence=root.resolve(System.getProperty("system.evidence.directory","docs/evidence/phase9-system"));
    final int serviceHeapMiB=Integer.getInteger("system.service.heapMiB",256);
    final ObjectMapper json=new ObjectMapper();final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    final Map<String,Integer> ports=new LinkedHashMap<>(),management=new LinkedHashMap<>();final Map<String,Process> processes=new LinkedHashMap<>();
    final List<Map<String,Object>> scenarios=new ArrayList<>();JdbcTemplate jdbc;String commit,adminToken;UUID customer=UUID.randomUUID(),product=UUID.randomUUID();int launches;
    final String secret="isolated-phase9-jwt-secret-".repeat(4),password="isolated-test-password";
    int port() throws Exception {try(var socket=new java.net.ServerSocket(0)){return socket.getLocalPort();}}
    @BeforeAll void start() throws Exception {
        Files.createDirectories(evidence);jdbc=new JdbcTemplate(new DriverManagerDataSource(postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword()));
        commit=command(root,"git","-c","safe.directory="+root.toString().replace('\\','/'),"rev-parse","HEAD").strip();
        for(String service:List.of("auth-service","order-service","inventory-service","notification-service","api-gateway")){ports.put(service,port());management.put(service,port());}
        for(String service:ports.keySet())launch(service,!Boolean.getBoolean("system.benchmark.only")&&(service.equals("order-service")||service.equals("inventory-service")),false);
        for(String service:ports.keySet())ready(service);
        jdbc.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");seed(customer,"admin@phase9.test","ADMIN");
        jdbc.update("INSERT INTO inventory_schema.inventory(product_id,product_name,quantity_available) VALUES (?,'Phase9 isolated stock',10000)",product);
        adminToken=login(ports.get("api-gateway"),"admin@phase9.test");
    }
    void seed(UUID id,String email,String role){jdbc.update("INSERT INTO auth_schema.users(id,email,password_hash) VALUES (?,?,crypt(?,gen_salt('bf',10)))",id,email,password);jdbc.update("INSERT INTO auth_schema.user_roles(user_id,role) VALUES (?,?)",id,role);}
    void launch(String service,boolean noRelay,boolean crash) throws Exception {
        Path jar=root.resolve(service+"/target/app"+(Set.of("inventory-service","notification-service").contains(service)?"-exec":"")+".jar");
        var args=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xms"+(service.equals("order-service")?serviceHeapMiB:64)+"m","-Xmx"+serviceHeapMiB+"m","-XX:ActiveProcessorCount=2","-Duser.timezone=UTC"));
        args.add("-Xlog:gc:file="+root.resolve(service).relativize(evidence.resolve(service+"-"+(launches+1)+"-gc.log")).toString().replace('\\','/'));
        if(crash){args.addAll(List.of("-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),InventoryCommitCrashProcess.class.getName()));}else args.addAll(List.of("-jar",jar.toString()));
        var builder=new ProcessBuilder(args).directory(root.resolve(service).toFile()).redirectErrorStream(true).redirectOutput(evidence.resolve(service+"-"+(++launches)+".jsonl").toFile());
        var env=builder.environment();env.put("SPRING_DATASOURCE_URL",postgres.getJdbcUrl());env.put("SPRING_DATASOURCE_USERNAME",postgres.getUsername());env.put("SPRING_DATASOURCE_PASSWORD",postgres.getPassword());
        env.put("SPRING_FLYWAY_ENABLED","true");env.put("SPRING_MAIN_BANNER_MODE","off");
        env.put("SPRING_KAFKA_BOOTSTRAP_SERVERS",broker.getBootstrapServers());env.put("REDIS_HOST",redis.getHost());env.put("REDIS_PORT",redis.getMappedPort(6379).toString());env.put("JWT_SECRET",secret);
        env.put("INTERNAL_SERVICE_TOKEN","phase9-private-service-credential-".repeat(2));env.put("SERVER_PORT",ports.get(service).toString());env.put("MANAGEMENT_PORT",management.get(service).toString());env.put("APP_ENV","phase9-test");
        env.put("LOGGING_FILE_NAME",evidence.resolve(service+"-audit.jsonl").toString());env.put("SPRING_JPA_SHOW_SQL","false");env.put("CLOUDWATCH_METRICS_ENABLED","false");
        for(String target:List.of("auth","order","inventory","notification"))env.put(target.toUpperCase(Locale.ROOT)+"_SERVICE_URL","http://127.0.0.1:"+ports.get(target+"-service"));
        env.put("ORDER_RELAY_ENABLED",Boolean.toString(!noRelay));env.put("INVENTORY_RELAY_ENABLED",Boolean.toString(!noRelay));
        processes.put(service,builder.start());
    }
    void ready(String service){await().alias(service+" readiness after bounded startup").atMost(Duration.ofMinutes(5)).pollInterval(Duration.ofMillis(500)).until(()->{
        if(!processes.get(service).isAlive())throw new IllegalStateException(service+" exited; inspect isolated service log");
        try{return send(management.get(service),"GET","/actuator/health/readiness",null,null,null).statusCode()==200;}catch(Exception e){return false;}
    });}
    void stop(String service) throws Exception {var process=processes.remove(service);if(process!=null&&process.isAlive()){process.destroyForcibly();assertThat(process.waitFor(15,TimeUnit.SECONDS)).isTrue();}}
    @AfterAll void stopAll() throws Exception {for(String service:new ArrayList<>(processes.keySet()))stop(service);}
    HttpResponse<String> send(int port,String method,String path,String token,String key,String body) throws Exception {
        var request=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+path)).timeout(Duration.ofSeconds(15)).header("X-Correlation-Id",UUID.randomUUID().toString());
        if(token!=null)request.header("Authorization","Bearer "+token);if(key!=null)request.header("Idempotency-Key",key);
        if(body!=null)request.header("Content-Type","application/json");request.method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(body));return http.send(request.build(),HttpResponse.BodyHandlers.ofString());
    }
    String login(int port,String email)throws Exception{var result=send(port,"POST","/auth/login",null,null,json.writeValueAsString(Map.of("email",email,"password",password)));assertThat(result.statusCode()).as(result.body()).isEqualTo(200);return json.readTree(result.body()).path("accessToken").asText();}
    UUID create(String token,String key,int quantity)throws Exception{var result=send(ports.get("api-gateway"),"POST","/api/orders",token,key,json.writeValueAsString(Map.of("productId",product,"quantity",quantity)));assertThat(result.statusCode()).as(result.body()).isEqualTo(201);return UUID.fromString(json.readTree(result.body()).path("id").asText());}
    String state(UUID order){return jdbc.queryForObject("SELECT status FROM order_schema.orders WHERE id=?",String.class,order);}
    void confirmed(UUID order){await().atMost(Duration.ofSeconds(30)).until(()->state(order).equals("CONFIRMED"));}
    long count(String table){return jdbc.queryForObject("SELECT count(*) FROM "+table,Long.class);}
    void record(String id,Instant start,String expected,Map<String,Object> observed)throws Exception{
        scenarios.add(Map.of("scenarioId",id,"commit",commit,"environment","isolated Testcontainers / five service JVMs","startedAt",start.toString(),"endedAt",Instant.now().toString(),"expectedInvariant",expected,"observed",observed,"result","PASS","artifacts",List.of(root.resolve("docs/evidence").relativize(evidence).toString().replace('\\','/')+"/")));
        Files.writeString(evidence.resolve("scenarios.json"),json.writerWithDefaultPrettyPrinter().writeValueAsString(scenarios));
    }
    String command(Path cwd,String...args)throws Exception{var process=new ProcessBuilder(args).directory(cwd.toFile()).redirectErrorStream(true).start();String output=new String(process.getInputStream().readAllBytes());assertThat(process.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).as(output).isZero();return output;}
    int inventoryRelay()throws Exception{
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),Phase9RelayProcess.class.getName(),postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword(),broker.getBootstrapServers(),"publish")
            .redirectErrorStream(true).redirectOutput(evidence.resolve("inventory-relay-"+(++launches)+".log").toFile()).start();
        try{assertThat(child.waitFor(40,TimeUnit.SECONDS)).isTrue();return child.exitValue();}finally{if(child.isAlive())child.destroyForcibly();}
    }
    @Test @Order(1) void T01_T09_T12_realServicesAndBrokerRecovery()throws Exception{
        Instant started=Instant.now();UUID order=create(adminToken,"durable-before-relay",2);assertThat(state(order)).isEqualTo("PENDING");
        stop("order-service");launch("order-service",false,false);ready("order-service");
        await().atMost(Duration.ofSeconds(30)).until(()->count("inventory_schema.inventory_reservations")==1);
        String immutable=jdbc.queryForObject("SELECT payload::text FROM inventory_schema.outbox_events WHERE aggregate_id=?",String.class,order);
        broker.getDockerClient().pauseContainerCmd(broker.getContainerId()).exec();
        try{assertThat(inventoryRelay()).isZero();assertThat(jdbc.queryForObject("SELECT status FROM inventory_schema.outbox_events WHERE aggregate_id=?",String.class,order)).isEqualTo("PENDING");
            assertThat(send(management.get("order-service"),"GET","/actuator/health/readiness",null,null,null).statusCode()).isEqualTo(503);
            assertThat(send(management.get("order-service"),"GET","/actuator/health/liveness",null,null,null).statusCode()).isEqualTo(200);
            assertThat(send(ports.get("api-gateway"),"POST","/api/orders",adminToken,"outage-rejected",json.writeValueAsString(Map.of("productId",product,"quantity",1))).statusCode()).isEqualTo(503);
        }
        finally{broker.getDockerClient().unpauseContainerCmd(broker.getContainerId()).exec();}
        assertThat(inventoryRelay()).isZero();confirmed(order);
        await().atMost(Duration.ofSeconds(20)).until(()->count("notification_schema.notifications")==2);
        assertThat(jdbc.queryForObject("SELECT quantity_available FROM inventory_schema.inventory WHERE product_id=?",Integer.class,product)).isEqualTo(9998);
        assertThat(jdbc.queryForObject("SELECT payload::text FROM inventory_schema.outbox_events WHERE aggregate_id=?",String.class,order)).isEqualTo(immutable);
        for(String scenario:List.of("T01","T09","T12"))record(scenario,started,"Committed order survives restart and broker outage; exactly one reservation and two notifications",Map.of("orderId",order,"stock",9998,"notifications",2));
        stop("inventory-service");launch("inventory-service",false,false);ready("inventory-service");
    }
    @Test @Order(2) void T11_inventoryProcessDiesAfterCommitBeforeAcknowledgement()throws Exception{
        Instant started=Instant.now();stop("inventory-service");launch("inventory-service",false,true);ready("inventory-service");
        UUID order=create(adminToken,"inventory-before-ack",2);Process doomed=processes.get("inventory-service");
        assertThat(doomed.waitFor(45,TimeUnit.SECONDS)).isTrue();assertThat(doomed.exitValue()).isEqualTo(73);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_schema.inventory_reservations WHERE order_id=?",Integer.class,order)).isEqualTo(1);
        launch("inventory-service",false,false);ready("inventory-service");confirmed(order);
        assertThat(jdbc.queryForObject("SELECT quantity_available FROM inventory_schema.inventory WHERE product_id=?",Integer.class,product)).isEqualTo(9996);
        record("T11",started,"Redelivery after committed reservation and process halt does not deduct stock twice",Map.of("orderId",order,"stock",9996,"exitCode",73));
    }
    @Test @Order(3) void T13_dltUnavailableRetainsSourceOffset()throws Exception{
        Instant started=Instant.now();String topic="order.placed.inventory-service.DLT";
        try(var admin=Admin.create(Map.of("bootstrap.servers",broker.getBootstrapServers()));var producer=new KafkaProducer<String,String>(Map.of("bootstrap.servers",broker.getBootstrapServers(),"acks","all"),new StringSerializer(),new StringSerializer())){
            var resource=new ConfigResource(ConfigResource.Type.TOPIC,topic);
            admin.incrementalAlterConfigs(Map.of(resource,List.of(new AlterConfigOp(new ConfigEntry("min.insync.replicas","2"),AlterConfigOp.OpType.SET)))).all().get(10,TimeUnit.SECONDS);
            var sent=producer.send(new ProducerRecord<>("order.placed",0,"phase9-invalid","{broken-phase9")).get(10,TimeUnit.SECONDS);var partition=new TopicPartition("order.placed",0);
            try{await().during(Duration.ofSeconds(12)).atMost(Duration.ofSeconds(18)).untilAsserted(()->{var offsets=admin.listConsumerGroupOffsets("inventory-service").partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS);var offset=offsets.get(partition);assertThat(offset==null?0:offset.offset()).isLessThanOrEqualTo(sent.offset());});}
            finally{admin.incrementalAlterConfigs(Map.of(resource,List.of(new AlterConfigOp(new ConfigEntry("min.insync.replicas","1"),AlterConfigOp.OpType.SET)))).all().get(10,TimeUnit.SECONDS);}
            await().atMost(Duration.ofSeconds(45)).untilAsserted(()->assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_schema.dead_letters WHERE source_topic='order.placed' AND source_partition=0 AND source_offset=?",Integer.class,sent.offset())).isEqualTo(1));
            await().atMost(Duration.ofSeconds(15)).until(()->admin.listConsumerGroupOffsets("inventory-service").partitionsToOffsetAndMetadata().get().get(partition).offset()>sent.offset());
            record("T13",started,"Offset advances only after DLT publication recovers",Map.of("sourceOffset",sent.offset(),"dltIndexed",true));
        }
    }
    @Test @Order(4) void T22_backupRestoresEveryOwnedTableAndMigrationVersion()throws Exception{
        Instant started=Instant.now();await().atMost(Duration.ofSeconds(30)).until(()->jdbc.queryForObject("SELECT count(*) FROM order_schema.outbox_events WHERE status='PENDING'",Long.class)==0);
        assertThat(postgres.execInContainer("pg_dump","-U",postgres.getUsername(),"-d",postgres.getDatabaseName(),"-Fc","-f","/tmp/phase9.dump").getExitCode()).isZero();
        assertThat(postgres.execInContainer("createdb","-U",postgres.getUsername(),"phase9_restored").getExitCode()).isZero();
        assertThat(postgres.execInContainer("pg_restore","-U",postgres.getUsername(),"--no-owner","-d","phase9_restored","/tmp/phase9.dump").getExitCode()).isZero();
        var restored=new JdbcTemplate(new DriverManagerDataSource(postgres.getJdbcUrl().replace("/"+postgres.getDatabaseName(),"/phase9_restored"),postgres.getUsername(),postgres.getPassword()));
        var tables=jdbc.queryForList("SELECT table_schema||'.'||table_name FROM information_schema.tables WHERE table_schema IN ('auth_schema','order_schema','inventory_schema','notification_schema') AND table_type='BASE TABLE'",String.class);
        for(String table:tables){String query="SELECT md5(COALESCE(string_agg(row_to_json(t)::text,'' ORDER BY row_to_json(t)::text),'')) FROM "+table+" t";assertThat(restored.queryForObject(query,String.class)).as(table).isEqualTo(jdbc.queryForObject(query,String.class));}
        record("T22",started,"Isolated PostgreSQL restore reconciles all rows and migration histories",Map.of("matchingTables",tables.size(),"database","phase9_restored"));
    }
    @Test @Order(5) void privateMetricsAndOwningServiceInvestigation()throws Exception{
        Instant started=Instant.now();
        for(String service:ports.keySet()){
            var scrape=send(management.get(service),"GET","/actuator/prometheus",null,null,null);assertThat(scrape.statusCode()).as(service).isEqualTo(200);assertThat(scrape.body()).contains("jvm_memory",service);
            assertThat(send(ports.get(service),"GET","/actuator/prometheus",adminToken,null,null).statusCode()).as(service).isEqualTo(404);
        }
        UUID order=jdbc.queryForObject("SELECT id FROM order_schema.orders ORDER BY created_at LIMIT 1",UUID.class);
        String url="http://127.0.0.1:"+ports.get("inventory-service")+"/internal/ops/orders/"+order;
        assertThat(http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(401);
        var result=http.send(HttpRequest.newBuilder(URI.create(url)).header("X-Service-Credential","phase9-private-service-credential-".repeat(2)).GET().build(),HttpResponse.BodyHandlers.ofString());
        assertThat(result.statusCode()).isEqualTo(200);assertThat(json.readTree(result.body()).path("decisions").size()).isEqualTo(1);
        record("OBS-INTERNAL",started,"Metrics stay on internal listeners; authenticated owning-service investigation is available",Map.of("services",5));
    }
    @Test @Order(6) void separateRealHttpIdempotencyContention()throws Exception{
        Instant started=Instant.now();String body=json.writeValueAsString(Map.of("productId",product,"quantity",1));
        var ready=new CountDownLatch(20);var go=new CountDownLatch(1);var responses=new ArrayList<Future<HttpResponse<String>>>();
        try(var pool=Executors.newFixedThreadPool(20)){
            for(int i=0;i<20;i++)responses.add(pool.submit(()->{ready.countDown();go.await();return send(ports.get("api-gateway"),"POST","/api/orders",adminToken,"phase9-contention",body);}));
            assertThat(ready.await(10,TimeUnit.SECONDS)).isTrue();go.countDown();var bodies=new HashSet<String>();int replays=0;
            for(var future:responses){var response=future.get(20,TimeUnit.SECONDS);assertThat(response.statusCode()).as(response.body()).isEqualTo(201);bodies.add(response.body());if(response.headers().firstValue("Idempotent-Replayed").orElse("").equals("true"))replays++;}
            assertThat(bodies).hasSize(1);assertThat(replays).isEqualTo(19);UUID order=UUID.fromString(json.readTree(bodies.iterator().next()).path("id").asText());confirmed(order);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_schema.outbox_events WHERE aggregate_id=? AND event_type='order.placed'",Integer.class,order)).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_schema.inventory_reservations WHERE order_id=?",Integer.class,order)).isEqualTo(1);
            record("IDEMPOTENCY-CONTENTION",started,"Twenty concurrent Gateway HTTP requests share one durable order and event",Map.of("httpRequests",20,"orders",1,"placedEvents",1,"replayResponses",replays));
        }
    }
    @Test @Order(7) @EnabledIfSystemProperty(named="phase9.benchmark",matches="true")
    void measuredTwentyUserWorkload()throws Exception{
        Instant started=Instant.now();var tokens=new ArrayList<String>();
        for(int i=0;i<20;i++){String email="load"+i+"@phase9.test";seed(UUID.randomUUID(),email,"CUSTOMER");tokens.add(login(ports.get("auth-service"),email));}
        UUID loadProduct=UUID.randomUUID();jdbc.update("INSERT INTO inventory_schema.inventory(product_id,product_name,quantity_available) VALUES (?,'Isolated benchmark',10000)",loadProduct);
        var samples=new ConcurrentLinkedQueue<Map<String,Object>>();var failures=new ConcurrentLinkedQueue<String>();Object sampleLock=new Object();Files.writeString(evidence.resolve("load-samples.jsonl"),"");
        long start=System.nanoTime(),warmupEnd=start+TimeUnit.SECONDS.toNanos(60),end=warmupEnd+TimeUnit.SECONDS.toNanos(300);
        var pool=Executors.newScheduledThreadPool(20);var jobs=new ArrayList<ScheduledFuture<?>>();
        for(int user=0;user<20;user++){final int index=user;jobs.add(pool.scheduleAtFixedRate(()->{
            long before=System.nanoTime();if(before>=end)return;boolean measured=before>=warmupEnd;String key=UUID.randomUUID().toString();
            try{var response=send(ports.get("api-gateway"),"POST","/api/orders",tokens.get(index),key,json.writeValueAsString(Map.of("productId",loadProduct,"quantity",1)));
                double elapsed=(System.nanoTime()-before)/1_000_000d;
                if(response.statusCode()!=201)failures.add("HTTP_"+response.statusCode());
                var sample=Map.<String,Object>of("measured",measured,"httpMs",elapsed,"status",response.statusCode(),"orderId",json.readTree(response.body()).path("id").asText(),"user",index);samples.add(sample);
                synchronized(sampleLock){Files.writeString(evidence.resolve("load-samples.jsonl"),json.writeValueAsString(sample)+"\n",StandardOpenOption.APPEND);}
            }catch(Exception e){failures.add(e.getClass().getSimpleName());}
        },user*100L,2000,TimeUnit.MILLISECONDS));}
        try{await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(365)).until(()->System.nanoTime()>=end);}
        finally{jobs.forEach(job->job.cancel(false));pool.shutdown();assertThat(pool.awaitTermination(30,TimeUnit.SECONDS)).isTrue();}
        await().atMost(Duration.ofSeconds(30)).until(()->jdbc.queryForObject("SELECT count(*) FROM order_schema.orders WHERE product_id=? AND status='PENDING'",Long.class,loadProduct)==0);
        var measured=samples.stream().filter(s->(Boolean)s.get("measured")).toList();var httpTimes=measured.stream().map(s->((Number)s.get("httpMs")).doubleValue()).sorted().toList();
        var confirmations=new ArrayList<Double>();for(var sample:measured)if(!sample.get("orderId").toString().isBlank())confirmations.add(jdbc.queryForObject("SELECT EXTRACT(EPOCH FROM h.recorded_at-o.created_at)*1000 FROM order_schema.orders o JOIN order_schema.order_history h ON h.order_id=o.id AND h.new_status='CONFIRMED' WHERE o.id=?",Double.class,UUID.fromString(sample.get("orderId").toString())));Collections.sort(confirmations);
        long orders=jdbc.queryForObject("SELECT count(*) FROM order_schema.orders WHERE product_id=?",Long.class,loadProduct),reserved=jdbc.queryForObject("SELECT count(*) FROM inventory_schema.inventory_reservations WHERE product_id=?",Long.class,loadProduct);
        await().atMost(Duration.ofSeconds(30)).until(()->jdbc.queryForObject("SELECT count(*) FROM notification_schema.notifications n JOIN order_schema.orders o ON o.id=n.order_id WHERE o.product_id=?",Long.class,loadProduct)==2*orders);
        long notifications=jdbc.queryForObject("SELECT count(*) FROM notification_schema.notifications n JOIN order_schema.orders o ON o.id=n.order_id WHERE o.product_id=?",Long.class,loadProduct);
        long domainEvents=jdbc.queryForObject("SELECT count(*) FROM order_schema.outbox_events e JOIN order_schema.orders o ON o.id=e.aggregate_id WHERE o.product_id=? AND e.status='PUBLISHED'",Long.class,loadProduct);
        int stock=jdbc.queryForObject("SELECT quantity_available FROM inventory_schema.inventory WHERE product_id=?",Integer.class,loadProduct);
        long duplicateEvents=jdbc.queryForObject("SELECT count(*) FROM (SELECT aggregate_id,event_type FROM order_schema.outbox_events WHERE aggregate_id IN (SELECT id FROM order_schema.orders WHERE product_id=?) GROUP BY aggregate_id,event_type HAVING count(*)<>1) t",Long.class,loadProduct);
        var machine=(com.sun.management.OperatingSystemMXBean)java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        var report=new LinkedHashMap<String,Object>();report.put("scenarioId","PERF-20-USERS");report.put("commit",commit);report.put("startedAt",started.toString());report.put("endedAt",Instant.now().toString());report.put("environment",Map.of("os",System.getProperty("os.name"),"java",System.getProperty("java.version"),"hostProcessors",machine.getAvailableProcessors(),"hostMemoryBytes",machine.getTotalMemorySize(),"serviceJvmProcessors",2,"serviceHeapMiB",serviceHeapMiB,"orderInitialHeapMiB",serviceHeapMiB,"images",List.of("postgres:15.6","confluentinc/cp-kafka:7.4.0","redis:7.2.4-alpine")));
        report.put("warmupSeconds",60);report.put("measuredSeconds",300);report.put("users",20);report.put("scheduledRequestsPerSecond",10);report.put("measuredRequests",measured.size());report.put("throughputPerSecond",measured.size()/300d);report.put("errors",failures);report.put("httpMilliseconds",Map.of("p50",percentile(httpTimes,.50),"p95",percentile(httpTimes,.95),"p99",percentile(httpTimes,.99)));report.put("confirmationMilliseconds",Map.of("p50",percentile(confirmations,.50),"p95",percentile(confirmations,.95),"p99",percentile(confirmations,.99)));
        report.put("invariants",Map.of("orders",orders,"reservations",reserved,"initialStock",10000,"finalStock",stock,"duplicateBusinessEvents",duplicateEvents,"notifications",notifications,"publishedOrderEvents",domainEvents));report.put("expectedInvariant","HTTP p95 <500ms; confirmation p95 <5000ms; one reservation per order; no missing or duplicate effects");report.put("artifacts",List.of(root.resolve("docs/evidence").relativize(evidence).toString().replace('\\','/')+"/load-samples.json"));
        boolean pass=failures.isEmpty()&&measured.size()==3000&&percentile(httpTimes,.95)<500&&percentile(confirmations,.95)<5000&&orders==samples.size()&&reserved==orders&&stock==10000-orders&&duplicateEvents==0&&notifications==2*orders&&domainEvents==2*orders;
        report.put("result",pass?"PASS":"FAIL");Files.writeString(evidence.resolve("performance.json"),json.writerWithDefaultPrettyPrinter().writeValueAsString(report));Files.writeString(evidence.resolve("load-samples.json"),json.writeValueAsString(samples));
        assertThat(pass).as(json.writeValueAsString(report)).isTrue();
    }
    double percentile(List<Double> values,double quantile){return values.isEmpty()?Double.POSITIVE_INFINITY:values.get(Math.min(values.size()-1,(int)Math.ceil(values.size()*quantile)-1));}
}
