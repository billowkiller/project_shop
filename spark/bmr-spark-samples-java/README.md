Calculate PV and UV from access logs using spark java.

LOG FORMAT:

    $remote_addr - [$time_local] "$request" $status $body_bytes_sent "$http_referer"  $http_cookie" $remote_user "$http_user_agent" $request_time  $host $msec

Example:

    10.81.78.220 - [04/Oct/2015:21:31:22 +0800] "GET /u2bmp.html?dm=37no6.com/003&ac=1510042131161237772&v=y88j6-1.0&rnd=1510042131161237772&ext_y88j6_tid=003&ext_y88j6_uid=1510042131161237772 HTTP/1.1" 200 54 "-" "-" 9CA13069CB4D7B836DC0B8F8FD06F8AF "ImgoTV-iphone/4.5.3.150815 CFNetwork/672.1.13 Darwin/14.0.0" 0.004 test.com.org 1443965482.737
