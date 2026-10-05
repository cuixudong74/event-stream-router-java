package com.github.cuixudong74.eventstreamrouterjava.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HTTP 端到端：结构版本不兼容被拒绝并返回定位到字段的原因；合法事件提交后返回位点。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiEndToEndTest {

    @TempDir
    static Path dataDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("router.data-dir", () -> dataDir.toString());
        registry.add("router.target-schema-version", () -> 1);
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    void endToEndIngestAndReject() {
        // 注册结构版本 v1
        ResponseEntity<Map> schemaResp = rest.postForEntity("/admin/schemas", Map.of(
                "version", 1,
                "allowExtraFields", false,
                "fields", List.of(
                        Map.of("name", "orderId", "type", "STRING", "required", true),
                        Map.of("name", "amount", "type", "NUMBER", "required", true))), Map.class);
        assertThat(schemaResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        // 激活规则集
        ResponseEntity<Map> ruleResp = rest.postForEntity("/admin/rulesets", List.of(
                Map.of("ruleId", "r1", "eventType", "order", "downstreamIds", List.of(), "priority", 1)),
                Map.class);
        assertThat(ruleResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ruleResp.getBody().get("effectiveFromSeq")).isEqualTo(1);

        // 未知结构版本 -> 422
        ResponseEntity<Map> unknown = rest.postForEntity("/events", Map.of(
                "eventId", "e1", "schemaVersion", 99, "partitionKey", "k1",
                "eventType", "order", "payload", Map.of()), Map.class);
        assertThat(unknown.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(unknown.getBody().get("message").toString()).contains("unsupported schemaVersion");

        // 缺必填字段 -> 422，原因定位到字段
        ResponseEntity<Map> invalid = rest.postForEntity("/events", Map.of(
                "eventId", "e2", "schemaVersion", 1, "partitionKey", "k1",
                "eventType", "order", "payload", Map.of("orderId", "o1")), Map.class);
        assertThat(invalid.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(invalid.getBody().get("message").toString())
                .contains("amount").contains("REQUIRED_MISSING");

        // 合法事件 -> 202 且分配位点
        ResponseEntity<Map> ok = rest.postForEntity("/events", Map.of(
                "eventId", "e3", "schemaVersion", 1, "partitionKey", "k1",
                "eventType", "order", "payload", Map.of("orderId", "o1", "amount", 5)), Map.class);
        assertThat(ok.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(ok.getBody().get("seq")).isEqualTo(1);

        // 状态端点可观测
        ResponseEntity<Map> status = rest.getForEntity("/status", Map.class);
        assertThat(status.getBody().get("committedSeq")).isEqualTo(1);
    }
}
