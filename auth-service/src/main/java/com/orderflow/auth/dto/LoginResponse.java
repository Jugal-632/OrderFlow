package com.orderflow.auth.dto;
import java.util.*;
public record LoginResponse(String accessToken,long expiresIn,UUID userId,Set<String> roles) {}
