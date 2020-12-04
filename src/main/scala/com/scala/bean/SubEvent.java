package scala.com.scala.bean;
//子事件流
public class SubEvent extends Event{
    /**
     * 业务场景：旅游经典的容量
     */
    private final double volume;

    public SubEvent(int id, String name, double price, double volume) {
        super(id, name, price);
        this.volume = volume;
    }

    public double getVolume() {
        return volume;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SubEvent &&
                super.equals(obj) &&
                ((SubEvent) obj).volume == volume;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + (int) volume;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("SubEvent(编号→")
                .append(getId())
                .append(", 名称→")
                .append(getName())
                .append(", 旅游景点的价格→")
                .append(getPrice())
                .append(", 旅游景点的容纳量→")
                .append(getVolume())
                .append(")");

        return builder.toString();
    }
}
