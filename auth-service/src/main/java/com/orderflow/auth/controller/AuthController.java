package com.orderflow.auth.controller;
import com.orderflow.auth.dto.*;
import com.orderflow.auth.service.AuthService;
import com.orderflow.security.Caller;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map;
@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {
    private final AuthService auth;
    @PostMapping("/login") public LoginResponse login(@RequestBody LoginRequest request) { return auth.login(request); }
    @PostMapping("/logout") public ResponseEntity<Void> logout(@RequestAttribute("caller") Caller caller) {
        auth.logout(caller);return ResponseEntity.noContent().build();
    }
    @GetMapping("/me") public Map<String,Object> me(@RequestAttribute("caller") Caller caller) {
        return Map.of("userId",caller.userId(),"roles",caller.roles());
    }
}
