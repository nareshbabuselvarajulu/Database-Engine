import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

/**
 * Created by shruti on 22/4/17.
 */
public class Main {

    public static void main(String args[]) throws ParseException, SQLException, IOException {
        String phase = args[1];
        if(phase.equals("--in-mem")) {
            InMem inmem = new InMem();
            inmem.main();
        }
        else if(phase.equals("--on-disk")) {
            OnDisk ondisk = new OnDisk();
            ondisk.main();
        }

    }
}
