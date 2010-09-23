package signal;

public class Satellite {
    public int id;
    public String model;
    public String country;
    public double currentLat;
    public double currentLong;
    
    public void Satellite(int identifier, String model_number, String owner)  {
        id = identifier;
        model = model_number;
        country = owner;
    }

}
