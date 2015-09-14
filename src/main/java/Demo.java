import java.text.BreakIterator;
import java.util.Locale;

/**
 * Created by fchen on 15-7-2.
 */
public class Demo {
    public static void main(String args[]) {
        Locale locale = Locale.CHINA;
        BreakIterator breakIterator =
                BreakIterator.getWordInstance(locale);

        String text = "据外媒报道，英国一位车主买了新车后以防被盗，便想到帮爱车进行贴膜伪装。";

        breakIterator.setText("据外媒报道，英国一位车主买了新车后以防被盗，便想到帮爱车进行贴膜伪装。");

        int boundaryIndex = breakIterator.first();
        while(boundaryIndex != BreakIterator.DONE) {
            System.out.println(boundaryIndex) ;
            boundaryIndex = breakIterator.next();
            System.out.println(text.toCharArray()[boundaryIndex-1]);
        }
    }
}
