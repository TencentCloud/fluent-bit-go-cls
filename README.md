# fluent-bit-go-cls

An output plugin (Go) for TencentYun Simple Log Service (CLS)

### Build

```
go build -buildmode=c-shared -o fluent-bit-go.so .
```

### Run as fluent-bit
```
fluent-bit -c example/fluent.conf -e fluent-bit-go.so
```


### Example Config

```
[OUTPUT]
        Name             fluent-bit-go-cls
        Match            *
        TopicID          YOUR_TOPIC_ID
        CLSEndPoint      YOUR_ENDPOINT
        AccessKeyID      YOUR_PROJECT_SK
        AccessKeySecret  YOUR_PROJECT_AK
```