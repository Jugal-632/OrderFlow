package com.orderflow.inventory;

import com.orderflow.security.ApiErrors;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/api/inventory")
public class InventoryController {
    private final JdbcTemplate jdbc;
    public InventoryController(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    @GetMapping public List<Map<String,Object>> stock(@RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size) {
        ApiErrors.page(page,size);
        return jdbc.queryForList("SELECT product_id,product_name,quantity_available,updated_at,created_at FROM inventory_schema.inventory ORDER BY created_at DESC,product_id DESC LIMIT ? OFFSET ?",size,page*size);
    }
    @GetMapping("/{productId}/reservations") public List<Map<String,Object>> reservations(@PathVariable UUID productId,
            @RequestParam(defaultValue="0") int page,@RequestParam(defaultValue="20") int size) {
        ApiErrors.page(page,size);
        return jdbc.queryForList("SELECT id,order_id,product_id,quantity,status,reserved_at,released_at FROM inventory_schema.inventory_reservations WHERE product_id=? ORDER BY reserved_at DESC,id DESC LIMIT ? OFFSET ?",productId,size,page*size);
    }
}
