package ru.gx.core.kafka;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.utils.BytesUtils;

@SuppressWarnings("unused")
@Accessors(chain = true)
@EqualsAndHashCode
@ToString
public class LongHeader implements Header {
    @NotNull
    private final String internalKey;

    private long internalValue;

    public LongHeader(@NotNull final String key, final long value) {
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
        return BytesUtils.longToBytes(this.internalValue);
    }

    public LongHeader setValue(final long value) {
        this.internalValue = value;
        return this;
    }
}
