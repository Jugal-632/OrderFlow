package com.orderflow.security;
@FunctionalInterface
public interface AccessAudit { void rejected(int status,String correlationId); }
