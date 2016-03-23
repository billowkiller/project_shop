# Calculate Monthly User Increment from logs

HDFS DATA:

```
/app/dc/spark/bce/idmapping/log/idmlogs/20150223/0000/idmapping.log.1
/app/dc/spark/bce/idmapping/log/idmlogs/20150223/0001/idmapping.log.1
/app/dc/spark/bce/idmapping/log/idmlogs/20150223/0002/idmapping.log.1
...
```

Log File:

```
{"ua":"yidian_zixun/4.0.2.5 CFNetwork/758.2.8 Darwin/15.0.0","uid":"1603170059331404062","time":1458147638000,"ip":"10.57.127.55","version":"1.1","tid":"a1003","uri":"/x.gif?dm=bce.baidu.com/a1003&ac=1603170059331404062&v=bce-1.0&rnd=1603170059331404062&ext_bce_tid=a1003&ext_bce_uid=1603170059331404062","pid":"","baiduid":"967B508DBCD597FB1B4C2303FFCB01A7","bduss":"","referer":"-"}
{"ua":"ykhq/2.61 CFNetwork/758.2.8 Darwin/15.0.0","uid":"1512041201511721778","time":1458148013000,"ip":"10.57.127.37","version":"1.1","tid":"a1003","uri":"/x.gif?dm=bce.baidu.com/a1003&ac=1512041201511721778&v=bce-1.0&rnd=1512041201511721778&ext_bce_tid=a1003&ext_bce_uid=1512041201511721778","pid":"","baiduid":"17B19FCD927196128F65964E4F8D8688","bduss":"","referer":"-"}
...
```

Target:

keep track of total user, and fetch montly user increment of some tid
