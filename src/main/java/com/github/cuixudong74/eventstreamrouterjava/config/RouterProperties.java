package com.github.cuixudong74.eventstreamrouterjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 路由服务配置。
 */
@ConfigurationProperties(prefix = "router")
public class RouterProperties {

    /** 数据目录（日志、检查点、注册表持久化）。 */
    private String dataDir = "data";

    /** 下游统一消费的目标结构版本。 */
    private int targetSchemaVersion = 1;

    /** 单下游允许的最大滞后量，超过则按 overflowPolicy 处理。 */
    private long maxLag = 10_000;

    /** 超出上界时的策略：REJECT_INGEST 拒绝新提交并说明原因。 */
    private OverflowPolicy overflowPolicy = OverflowPolicy.REJECT_INGEST;

    /** 投递失败重试间隔（毫秒）。 */
    private long retryBackoffMillis = 200;

    public enum OverflowPolicy { REJECT_INGEST }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getTargetSchemaVersion() { return targetSchemaVersion; }
    public void setTargetSchemaVersion(int v) { this.targetSchemaVersion = v; }
    public long getMaxLag() { return maxLag; }
    public void setMaxLag(long maxLag) { this.maxLag = maxLag; }
    public OverflowPolicy getOverflowPolicy() { return overflowPolicy; }
    public void setOverflowPolicy(OverflowPolicy p) { this.overflowPolicy = p; }
    public long getRetryBackoffMillis() { return retryBackoffMillis; }
    public void setRetryBackoffMillis(long v) { this.retryBackoffMillis = v; }
}
