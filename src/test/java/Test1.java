import com.atguigu.edu.realtime.bean.DwsUserLoginWindowBean;
import com.atguigu.edu.realtime.util.DateFormatUtil;

/**
 * description:
 * Created by 铁盾 on 2022/6/22
 */
public class Test1 {
    public static void main(String[] args) {
        String curDate = "2022-02-22";
        String lastLoginDt = "2022-02-01";
        Long uvCount = 0L;
        Long backCount = 0L;
        if (lastLoginDt == null) {
//            lastLoginDtState.update(curDate);
            uvCount = 1L;
        } else {
            if (!lastLoginDt.equals(curDate)) {
//                lastLoginDtState.update(curDate);
                uvCount = 1L;
                if ((DateFormatUtil.toTs(curDate)) - DateFormatUtil.toTs(lastLoginDt) / 1000 / 3600 / 24 >= 8) {
                    backCount = 1L;
                }
            }
        }
        if (uvCount != 0L || backCount != 0L) {
            DwsUserLoginWindowBean bean = DwsUserLoginWindowBean.builder()
                    .backCount(backCount)
                    .uvCount(uvCount)
                    .build();
            System.out.println("uvCount = " + uvCount);
            System.out.println("backCount = " + backCount);
        }
    }
}
