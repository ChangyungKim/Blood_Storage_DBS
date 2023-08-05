import javax.xml.transform.Result;
import java.sql.*;
import java.util.Random;
import java.util.Scanner;
public class Main {
    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            String url = "jdbc:mysql://localhost:3306/blood_storage";
            String username = "root";
            String password = "%041126karenina";

            Connection conn = DriverManager.getConnection(url, username, password);
            System.out.println("Database connected successfully!");

            Scanner sc=new Scanner(System.in);
            int choice;
            do{
                System.out.println("1. Create table");
                System.out.println("2. Insert record");
                System.out.println("3. Making Bitmap Index");
                System.out.println("4. Multiple Key Query");
                System.out.println("5. Count Query");
                System.out.println("6. Exit");
                System.out.print("Enter your choice: ");
                choice = sc.nextInt();

                switch(choice){
                    case 1:
                        createTable(conn);
                        break;
                    case 2:
                        insertRecord(conn);
                        updateDuplicateDonors(conn);
                        break;
                    case 3:
                        makeBitmapgenderIndex(conn);
                        makeBitmapbloodtypeIndex(conn);
                        break;
                    case 4:
                        multiplekeyQuery(conn);
                        break;
                    case 5:
                        countQuery(conn);
                        break;
                    case 6:
                        System.out.println("Exiting...");
                        break;
                    default:
                        System.out.println("Invalid choice, please try again.");
                }
            }while(choice!=6);
            conn.close();

        } catch (Exception e) {
            System.err.println("Failed to connect to database");
            e.printStackTrace();
        }
    }

    public static void createTable(Connection conn) throws SQLException {
        String sql="CREATE TABLE blood_donations (serial_num SERIAL PRIMARY KEY, center_id INT, donor_id INT, donor_name VARCHAR(255), donor_gender CHAR(1), blood_type VARCHAR(5), donation_date DATE)";
        Statement stmt=conn.createStatement();
        stmt.executeUpdate(sql);
        System.out.println("Table created successfully!");
    }

    public static void insertRecord(Connection conn) throws SQLException {
        String[] bloodTypes = {"A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"};
        String[] genders = {"M", "F"};
        Random rand = new Random();

        for(int i=0;i<1000;i++) {
            int centerId=rand.nextInt(100)+1;
            int donorId=rand.nextInt(1000000)+1;
            String donorName="";
            for(int j=0;j<rand.nextInt(5)+3;j++){
                donorName+=(char)('a'+rand.nextInt(26));
            }
            String donorGender=genders[rand.nextInt(2)];
            String bloodType=bloodTypes[rand.nextInt(8)];
            long startDate = Date.valueOf("2013-05-07").getTime();
            long endDate = Date.valueOf("2023-05-07").getTime();
            long randomTime = startDate + (long) (rand.nextDouble() * (endDate - startDate));
            Date donationDate= new Date(randomTime);

            String sql = "INSERT INTO blood_donations (center_id, donor_id, donor_name, donor_gender, blood_type, donation_date) VALUES (?, ?, ?, ?, ?, ?)";
            PreparedStatement pstmt=conn.prepareStatement(sql);
            pstmt.setInt(1, centerId);
            pstmt.setInt(2,donorId);
            pstmt.setString(3, donorName);
            pstmt.setString(4, donorGender);
            pstmt.setString(5,bloodType);
            pstmt.setDate(6, donationDate);
            pstmt.executeUpdate();

        }
        System.out.println("Record Inserted successfully!");
    }
    public static void updateDuplicateDonors(Connection conn) throws SQLException {
        String sql="SELECT donor_id, donor_name, donor_gender, blood_type FROM blood_donations GROUP BY donor_id HAVING COUNT(*) > 1";
        PreparedStatement pstmt=conn.prepareStatement(sql);
        ResultSet rs=pstmt.executeQuery();

        while(rs.next()){
            int donorId=rs.getInt("donor_id");
            String donorName=rs.getString("donor_name");
            String donorGender=rs.getString("donor_gender");
            String bloodType=rs.getString("blood_type");
            sql="UPDATE blood_donations SET donor_name = ?, donor_gender = ?, blood_type = ? WHERE donor_id = ?";
            pstmt=conn.prepareStatement(sql);
            pstmt.setString(1, donorName);
            pstmt.setString(2, donorGender);
            pstmt.setString(3, bloodType);
            pstmt.setInt(4, donorId);
            pstmt.executeUpdate();

        }
        System.out.println("Duplicate donors updated successfully!");
    }

    public static void makeBitmapgenderIndex(Connection conn) throws SQLException {
        String indexTable = "donor_gender_index";
        String createTableSQL = "CREATE TABLE " + indexTable + " (gender CHAR(1), bitmap LONGTEXT)";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(createTableSQL);
        String[] genders = {"M", "F"};

        for (String gender : genders) {
            String selectRecordsSQL = "SELECT serial_num FROM blood_donations WHERE donor_gender = ?";
            PreparedStatement recordsPstmt = conn.prepareStatement(selectRecordsSQL);
            recordsPstmt.setString(1, gender);
            ResultSet recordsRs = recordsPstmt.executeQuery();

            int number=getRecordCount(conn);
            StringBuilder bitmap = new StringBuilder();
            for (int i = 0; i < number; i++) {
                bitmap.append("0");
            }

            while (recordsRs.next()) {
                int serialNum = recordsRs.getInt("serial_num") - 1;
                bitmap.setCharAt(serialNum, '1');
            }

            String insertIndexSQL = "INSERT INTO " + indexTable + " (gender, bitmap) VALUES (?, ?)";
            PreparedStatement insertPstmt = conn.prepareStatement(insertIndexSQL);
            insertPstmt.setString(1, gender);
            insertPstmt.setString(2, bitmap.toString());
            insertPstmt.executeUpdate();
        }
        System.out.println("Gender Bitmap index created successfully!");
    }

    public static void makeBitmapbloodtypeIndex(Connection conn) throws SQLException{
        String indexTable = "blood_type_index";
        String createTableSQL = "CREATE TABLE " + indexTable + " (blood_type VARCHAR(3), bitmap LONGTEXT)";
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(createTableSQL);
        String[] blood_types = {"A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"};

        for (String blood_type : blood_types) {
            String selectRecordsSQL = "SELECT serial_num FROM blood_donations WHERE blood_type = ?";
            PreparedStatement recordsPstmt = conn.prepareStatement(selectRecordsSQL);
            recordsPstmt.setString(1, blood_type);
            ResultSet recordsRs = recordsPstmt.executeQuery();

            int number=getRecordCount(conn);
            StringBuilder bitmap = new StringBuilder();
            for (int i = 0; i < number; i++) {
                bitmap.append("0");
            }
            while (recordsRs.next()) {
                int serialNum = recordsRs.getInt("serial_num") - 1;
                bitmap.setCharAt(serialNum, '1');
            }

            String insertIndexSQL = "INSERT INTO " + indexTable + " (blood_type, bitmap) VALUES (?, ?)";
            PreparedStatement insertPstmt = conn.prepareStatement(insertIndexSQL);
            insertPstmt.setString(1, blood_type);
            insertPstmt.setString(2, bitmap.toString());
            insertPstmt.executeUpdate();
        }

        System.out.println("Blood type Bitmap index created successfully!");
    }
    public static int getRecordCount(Connection conn) throws SQLException{
        String countSQL = "SELECT COUNT(*) AS count FROM blood_donations";
        Statement countStmt = conn.createStatement();
        ResultSet countRs = countStmt.executeQuery(countSQL);
        countRs.next();
        return countRs.getInt("count");
    }

    public static void multiplekeyQuery(Connection conn) throws SQLException {
        Scanner sc = new Scanner(System.in);
        String attribute = null;
        System.out.printf("Enter gender (M/F) : ");
        String gender = sc.nextLine();
        System.out.printf("Enter blood type(A+, A-, B+, B-, O+, O-, AB+, AB-) : ");
        String bloodType = sc.nextLine();
        System.out.printf("Enter bit operation(AND, OR, NOT) : ");
        String operation = sc.nextLine();
        if (operation.equals("NOT")) {
            System.out.printf("Enter attribute(Gender, BloodType) : ");
            attribute = sc.nextLine();
        }
        String genderindexQuery = "SELECT bitmap FROM donor_gender_index WHERE gender=?";
        PreparedStatement genderpstmt = conn.prepareStatement(genderindexQuery);
        genderpstmt.setString(1, gender);
        ResultSet genderRs = genderpstmt.executeQuery();

        String bloodtypeindexQuery = "SELECT bitmap FROM blood_type_index WHERE blood_type=?";
        PreparedStatement bloodtypepstmt = conn.prepareStatement(bloodtypeindexQuery);
        bloodtypepstmt.setString(1, bloodType);
        ResultSet bloodtypeRs = bloodtypepstmt.executeQuery();

        if (genderRs.next() && bloodtypeRs.next()) {
            String genderBitmap = genderRs.getString("bitmap");
            String bloodTypeBitmap = bloodtypeRs.getString("bitmap");
            StringBuilder resultBitmap = new StringBuilder();

            if (operation.equals("AND")) {
                for (int i = 0; i < genderBitmap.length(); i++) {
                    if (genderBitmap.charAt(i) == '1' && bloodTypeBitmap.charAt(i) == '1') {
                        resultBitmap.append("1");
                    } else {
                        resultBitmap.append("0");
                    }
                }
            } else if (operation.equals("OR")) {
                for (int i = 0; i < genderBitmap.length(); i++) {
                    if (genderBitmap.charAt(i) == '1' || bloodTypeBitmap.charAt(i) == '1') {
                        resultBitmap.append("1");
                    } else {
                        resultBitmap.append("0");
                    }
                }
            } else if (operation.equals("NOT")) {
                if (attribute.equals("Gender")) {
                    for (int i = 0; i < genderBitmap.length(); i++) {
                        if (genderBitmap.charAt(i) == '0') {
                            resultBitmap.append("1");
                        } else {
                            resultBitmap.append("0");
                        }
                    }
                } else if (attribute.equals("BloodType")) {
                    for (int i = 0; i < bloodTypeBitmap.length(); i++) {
                        if (bloodTypeBitmap.charAt(i) == '0') {
                            resultBitmap.append("1");
                        } else {
                            resultBitmap.append("0");
                        }
                    }
                }
            } else {
                System.out.println("다시 제대로 입력해주세요");
                return;
            }
            System.out.println("Result Bitmap : " + resultBitmap.toString());

            for (int i = 0; i < resultBitmap.length(); i++) {
                if (resultBitmap.charAt(i) == '1') {
                    printRecord(conn, i + 1);
                }
            }
        }
    }
    public static void printRecord(Connection conn, int serialNum) throws SQLException{
        String selectRecord="SELECT donor_name, donation_date FROM blood_donations WHERE serial_num=?";
        PreparedStatement pstmt=conn.prepareStatement(selectRecord);
        pstmt.setInt(1, serialNum);
        ResultSet rs=pstmt.executeQuery();

        if(rs.next()){
            String donorName=rs.getString("donor_name");
            String donationDate=rs.getString("donation_date");
            System.out.println("기부자 : "+donorName+ ", 기부 일자 : " +  donationDate);
        }
    }

    public static void countQuery(Connection conn) throws SQLException{
        Scanner sc=new Scanner(System.in);
        String gender=null;
        String bloodtype=null;
        System.out.print("Enter attribute (Gender/BloodType) : ");
        String attribute=sc.nextLine();
        if(attribute.equals("Gender")){
            System.out.print("Enter gender (M/F) : ");
            gender=sc.nextLine();
            String temp_query="SELECT bitmap FROM donor_gender_index WHERE gender = ?";
            PreparedStatement temp_pstmt=conn.prepareStatement(temp_query);
            temp_pstmt.setString(1, gender);
            ResultSet temp_rs=temp_pstmt.executeQuery();

            if(temp_rs.next()){
                String bitmap=temp_rs.getString("bitmap");
                int count=0;
                for(int i=0;i<bitmap.length();i++){
                    if(bitmap.charAt(i)=='1'){
                        count++;
                    }
                }
                System.out.println("성별이 " +gender+" 인 사람의 수 : "+count);
            }else{
                System.out.println("해당 성별에 관한 사람이 없습니다");
            }
        }else if(attribute.equals("BloodType")){
            System.out.print("Enter blood type(A+, A-, B+, B-, O+, O-, AB+, AB-) : ");
            bloodtype=sc.nextLine();
            String temp_query="SELECT bitmap FROM blood_type_index WHERE blood_type= ?";
            PreparedStatement temp_pstmt=conn.prepareStatement(temp_query);
            temp_pstmt.setString(1, bloodtype);
            ResultSet temp_rs=temp_pstmt.executeQuery();

            if(temp_rs.next()){
                String bitmap=temp_rs.getString("bitmap");
                int count=0;
                for(int i=0;i<bitmap.length();i++){
                    if(bitmap.charAt(i)=='1'){
                        count++;
                    }
                }
                System.out.println("혈액형이 " +bloodtype+" 인 사람의 수 : "+count);
            }else{
                System.out.println("해당 혈액형인 사람이 없습니다");
            }
        }else{
            System.out.print("다시 제대로 입력해주세요!");
        }


    }


}
