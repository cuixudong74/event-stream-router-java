package com.github.cuixudong74.eventstreamrouterjava.core;

import java.util.List;

/**
 * 结构校验结果：不通过时携带逐字段错误，绝不静默丢弃或补默认值放行。
 */
public record ValidationResult(boolean valid, List<FieldError> errors) {

    public static ValidationResult ok() {
        return new ValidationResult(true, List.of());
    }

    public static ValidationResult failed(List<FieldError> errors) {
        return new ValidationResult(false, List.copyOf(errors));
    }
}
