import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by franCiS on Mar 05, 2015.
 */
public class MapTest {
    public static void main(String[] args) {
        Map m = new HashMap();
        m.put("1", 1);
        m.put("2", 2);
        m.put("3",3);

        Iterator it = m.keySet().iterator();
        while (it.hasNext()) {
            System.out.println(m.get(it.next()));
        }
    }
}
