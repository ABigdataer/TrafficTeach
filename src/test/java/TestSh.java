import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TestSh {

    /**
     * java代码中如何执行liunx脚本？
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{

        String sh = "/root/test.sh" + args[0] + args[1];

        Process proc = Runtime.getRuntime().exec(sh);

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String flag;
        while ((flag = bufferedReader.readLine()) != null)
        {
            System.out.println("result" + flag);
        }
        bufferedReader.close();

        proc.waitFor();
    }

}
