package ru.gx.kafka.events;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.DataObject;

@Getter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class NewOldDataObjectsPair<T extends DataObject> {
    /**
     * Ключ данной пары объектов.
     */
    @Nullable
    private final Object key;

    /**
     * Новый объект при загрузке.
     */
    @Setter
    @NotNull
    private T newObject;

    /**
     * Старый объект, который соответствует новому. Если null, то старого объекта нет.
     */
    @Nullable
    private final T oldObject;

    /**
     * Действие, которое требуется совершить с новым объектом.
     */
    @Setter
    @NotNull
    private ActionOnChangingDueLoading action;

    /**
     * Равны ли старый и новый объекты.
     */
    private final boolean isEqual;

    public NewOldDataObjectsPair(@Nullable final Object key, @NotNull final T newObject, @Nullable final T oldObject, @NotNull final ActionOnChangingDueLoading action) {
        this.key = key;
        this.newObject = newObject;
        this.oldObject = oldObject;
        this.action = action;
        this.isEqual = (this.oldObject != null && this.oldObject.equals(this.newObject));
    }
}
