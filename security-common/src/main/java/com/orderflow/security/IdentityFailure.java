package com.orderflow.security;

public class IdentityFailure extends RuntimeException {
    public final int status;
    public final String code;
    public IdentityFailure(int status,String code) { super(code); this.status=status; this.code=code; }
}
