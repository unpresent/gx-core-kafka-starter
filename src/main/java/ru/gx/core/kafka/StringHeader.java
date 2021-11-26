package ru.gx.core.kafka;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;

@Accessors(chain = true)
@EqualsAndHashCode
@ToString
public class StringHeader implements Header {
    @NotNull
    private final String internalKey;

    @Nullable
    private String internalValue;

    public StringHeader(@NotNull final String key, @Nullable final String value) {
        super();
        this.internalKey = key;
        this.internalValue = value;
    }

    @Override
    public String key() {
        return this.internalKey;
    }

    @Override
    public byte[] value() {
        return StringUtils.isEmpty(this.internalValue) ? null : this.internalValue.getBytes(StandardCharsets.UTF_8);
    }

    public StringHeader setValue(@NotNull final String value) {
        this.internalValue = value;
        return this;
    }
}
