package ru.gxfin.common.kafka.events;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gxfin.common.data.DataObject;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class NewOldDataObjectsPair<T extends DataObject> {
    /**
     * Ключ данной пары объектов.
     */
    @Getter
    private final Object key;

    /**
     * Новый объект при загрузке.
     */
    @Getter
    @Setter
    private T newObject;

    /**
     * Старый объект, который соответствует новому. Если null, то старого объекта нет.
     */
    @Getter
    private final T oldObject;

    /**
     * Действие, которое требуется совершить с новым объектом.
     */
    @Getter
    @Setter
    private ActionOnChangingDueLoading action;

    /**
     * Равны ли старый и новый объекты.
     */
    @Getter
    private final boolean isEqual;

    public NewOldDataObjectsPair(@Nullable Object key, @NotNull T newObject, @Nullable T oldObject, ActionOnChangingDueLoading action) {
        this.key = key;
        this.newObject = newObject;
        this.oldObject = oldObject;
        this.action = action;
        this.isEqual = (this.oldObject != null && this.oldObject.equals(this.newObject));
    }
}
