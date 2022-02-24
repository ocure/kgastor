package kgastor.utils;
import java.util.concurrent.TimeUnit;

public class Evaluation {
    private long executeTimeOrigin;
    private long executeTimePublic;
    private long executeTimePrivate;
    private long originTriples;
    private long publicTriples;
    private long privateTriples;
    private long originSize;
    private long publicSize;
    private long privateSize;

    public Evaluation() {
    }

    public Evaluation(long executeTimeOrigin, long executeTimePublic, long executeTimePrivate, long originTriples, long publicTriples, long privateTriples, long originSize, long publicSize, long privateSize) {
        this.executeTimeOrigin = executeTimeOrigin;
        this.executeTimePublic = executeTimePublic;
        this.executeTimePrivate = executeTimePrivate;
        this.originTriples = originTriples;
        this.publicTriples = publicTriples;
        this.privateTriples = privateTriples;
        this.originSize = originSize;
        this.publicSize = publicSize;
        this.privateSize = privateSize;
    }

    public long getExecuteTimeOrigin() {
        return executeTimeOrigin;
    }

    public void setExecuteTimeOrigin(long executeTimeOrigin) {
        this.executeTimeOrigin = executeTimeOrigin;
    }

    public long getExecuteTimePublic() {
        return executeTimePublic;
    }

    public void setExecuteTimePublic(long executeTimePublic) {
        this.executeTimePublic = executeTimePublic;
    }

    public long getExecuteTimePrivate() {
        return executeTimePrivate;
    }

    public void setExecuteTimePrivate(long executeTimePrivate) {
        this.executeTimePrivate = executeTimePrivate;
    }

    public long getOriginTriples() {
        return originTriples;
    }

    public void setOriginTriples(long originTriples) {
        this.originTriples = originTriples;
    }

    public long getPublicTriples() {
        return publicTriples;
    }

    public void setPublicTriples(long publicTriples) {
        this.publicTriples = publicTriples;
    }

    public long getPrivateTriples() {
        return privateTriples;
    }

    public void setPrivateTriples(long privateTriples) {
        this.privateTriples = privateTriples;
    }

    public long getOriginSize() {
        return originSize;
    }

    public void setOriginSize(long originSize) {
        this.originSize = originSize;
    }

    public long getPublicSize() {
        return publicSize;
    }

    public void setPublicSize(long publicSize) {
        this.publicSize = publicSize;
    }

    public long getPrivateSize() {
        return privateSize;
    }

    public void setPrivateSize(long privateSize) {
        this.privateSize = privateSize;
    }

    @Override
    public String toString() {
        return "Evaluation{" +
                "executeTimeOrigin=" + executeTimeOrigin +
                ", executeTimePublic=" + executeTimePublic +
                ", executeTimePrivate=" + executeTimePrivate +
                ", originTriples=" + originTriples +
                ", publicTriples=" + publicTriples +
                ", privateTriples=" + privateTriples +
                ", originSize=" + originSize +
                ", publicSize=" + publicSize +
                ", privateSize=" + privateSize +
                '}';
    }

    public static String showTime(long executeTime) {
        String timeFormat = "%02d days - %02dh:%02dm:%02ds:%03dms";
        long intMillis = executeTime;

        long days = TimeUnit.MILLISECONDS.toDays(intMillis);
        intMillis -= TimeUnit.DAYS.toMillis(days);

        long hours = TimeUnit.MILLISECONDS.toHours(intMillis);
        intMillis -= TimeUnit.HOURS.toMillis(hours);

        long minutes = TimeUnit.MILLISECONDS.toMinutes(intMillis);
        intMillis -= TimeUnit.MINUTES.toMillis(minutes);

        long seconds = TimeUnit.MILLISECONDS.toSeconds(intMillis);
        intMillis -= TimeUnit.SECONDS.toMillis(seconds);

        return String.format(timeFormat, days, hours, minutes, seconds, intMillis);
    }
}
