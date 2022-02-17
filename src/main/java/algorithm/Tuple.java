package algorithm;

public class Tuple {

    private final String id;
    private final int classId;
    private final int age;
    private String zipcode;
    private final String sa;
    private final boolean isDead;
    private final boolean isFake;
    //TODO Rajouter un champ pour l'alias


    public Tuple(String id, int classId, int age, String zipcode, String sa, boolean isDead, boolean isFake) {
        this.id = id;
        this.classId = classId;
        this.age = age;
        this.zipcode = zipcode;
        this.sa = sa;
        this.isDead = isDead;
        this.isFake = isFake;
    }


    public Tuple(String sa){
        id = "Fake";
        classId = -1;
        age = 0;
        zipcode = null;
        this.sa = sa;
        isDead = false;
        this.isFake = true;
    }



    public String getId() {
        return id;
    }

    public int getClassId() {
        return classId;
    }

    public int getAge() {
        return age;
    }

    public String getZipcode() {
        return zipcode;
    }

    public String getSa() {
        return sa;
    }

    public boolean getIsDead(){
        return isDead;
    }

    public boolean getIsFake() { return isFake;}


    public void setZipcode(String zipcode) {
        this.zipcode = zipcode;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Tuple { ").append(id).append(", ").append(age).append(", ").append(zipcode).append(", ")
                .append(sa).append("}");
        return sb.toString();
    }


}
