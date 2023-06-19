package online.githuboy.lagou.course;

import lombok.extern.slf4j.Slf4j;
import online.githuboy.lagou.course.support.BigCourseDownloader;
import online.githuboy.lagou.course.support.ExecutorService;
import online.githuboy.lagou.course.utils.ConfigUtil;

import java.io.IOException;

/**
 * 启动类
 * 下载训练营课程
 *
 * @author jack
 * @since 2021年5月20日
 */
@Slf4j
public class App_XunLianYing {
    public static void main(String[] args) throws IOException, InterruptedException {

        // 拉钩训练营课程ID
        String courseId = "4";
        //  视频保存的目录
        String savePath = ConfigUtil.readValue("mp4_xunlianying_dir");

        BigCourseDownloader downloader = new BigCourseDownloader(courseId, savePath);

        downloader.start();
        ExecutorService.tryTerminal();
    }
}
