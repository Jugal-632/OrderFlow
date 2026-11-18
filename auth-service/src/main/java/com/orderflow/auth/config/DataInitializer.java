package com.orderflow.auth.config;
import com.orderflow.auth.model.User;
import com.orderflow.auth.repository.UserRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
/** Inject bootstrap credentials; never reset existing passwords at startup. */
@Component
public class DataInitializer implements CommandLineRunner {
    private final UserRepository users; private final PasswordEncoder encoder; private final JdbcTemplate jdbc;
    private final String customerEmail,customerPassword,adminEmail,adminPassword;
    public DataInitializer(UserRepository users,PasswordEncoder encoder,JdbcTemplate jdbc,
            @Value("${bootstrap.customer.email:}") String customerEmail,@Value("${bootstrap.customer.password:}") String customerPassword,
            @Value("${bootstrap.admin.email:}") String adminEmail,@Value("${bootstrap.admin.password:}") String adminPassword) {
        this.users=users;this.encoder=encoder;this.jdbc=jdbc;this.customerEmail=customerEmail;
        this.customerPassword=customerPassword;this.adminEmail=adminEmail;this.adminPassword=adminPassword;
    }
    @Override @Transactional public void run(String... args) {
        seed(customerEmail,customerPassword,"CUSTOMER");seed(adminEmail,adminPassword,"ADMIN");
    }
    private void seed(String email,String password,String role) {
        if(email.isBlank() && password.isBlank()) return;
        if(email.isBlank() || password.length()<12) throw new IllegalStateException("Bootstrap requires email and password of at least 12 characters");
        var user=users.findByEmail(email).orElseGet(()->users.saveAndFlush(User.builder().email(email)
                .passwordHash(encoder.encode(password)).createdAt(java.time.Instant.now()).build()));
        jdbc.update("INSERT INTO auth_schema.user_roles(user_id,role) VALUES (?,?) ON CONFLICT DO NOTHING",user.getId(),role);
    }
}
