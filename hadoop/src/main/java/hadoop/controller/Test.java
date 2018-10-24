package hadoop.controller;

import hadoop.service.HBaseService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * @author alan.huang
 */
@RestController
@RequestMapping("/api")
public class Test {

    @Autowired
    private FsShell shell;

    @Autowired
    private Configuration configuration;

    @Autowired
    private HBaseService hBaseService;

    @GetMapping("/test")
    public void test() {
        for (FileStatus s : shell.lsr("/")) {
            System.out.println("> " + s.getPath());
        }
//        shell.put("E:/MiFlashvcom.ini","/user/input/");
    }


    @GetMapping("/")
    public String index(HttpServletRequest request) {
        String ip = request.getRemoteAddr();
        return ip;
    }

    @GetMapping("/hbase")
    public String createTable() throws Exception {
        hBaseService.createTable("ota_pre_record", "info");
        return "create table success!";
    }


    @GetMapping("/hbase/scan")
    public String scanRegexRowKey() {
        String regexKey = "^.*\\+15022176018\\+20900$";
        List<Cell> result = hBaseService.scanRegexRowKey("ota_pre_record", regexKey);
        if (null == result) {
            System.out.println("result is null");
        }
        for (Cell cell : result) {
            System.out.println("rowKey:" + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
            System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
            System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
            System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            System.out.println("Timestamp:" + cell.getTimestamp());
        }
        return "scan success";
    }


    @GetMapping("/oo")
    public void oo(HttpServletRequest request) {
        hBaseService.aa();
    }

}
