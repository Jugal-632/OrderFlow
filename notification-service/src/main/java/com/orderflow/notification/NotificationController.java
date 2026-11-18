package com.orderflow.notification;

import com.orderflow.security.ApiErrors;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {
    private final JdbcTemplate jdbc;
    public NotificationController(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    @GetMapping public List<Map<String,Object>> history(@RequestParam(required=false) UUID orderId,
            @RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size) {
        ApiErrors.page(page,size);
        String columns="SELECT id,order_id,event_type,message,recorded_at,order_version FROM notification_schema.notifications";
        if(orderId==null) return jdbc.queryForList(columns+" ORDER BY recorded_at DESC,id DESC LIMIT ? OFFSET ?",size,page*size);
        return jdbc.queryForList(columns+" WHERE order_id=? ORDER BY recorded_at DESC,id DESC LIMIT ? OFFSET ?",orderId,size,page*size);
    }
}
