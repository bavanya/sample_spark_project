import scala.Serializable;

public class Schema implements Serializable {
    String name;
    String location;
    String year_established;
    String area;

    // constructor , getters and setters

    public Schema(String name, String location, String year_established, String area){
        this.name = name;
        this.location = location;
        this.year_established = year_established;
        this.area = area;
    }

    public String getName(){
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocation(){
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getYear_established() {
        return year_established;
    }

    public void setYear_established(String year_established) {
        this.year_established = year_established;
    }
}
