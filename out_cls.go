package main

import "C"
import (
	"fmt"
	"github.com/fluent/fluent-bit-go/output"
	cls "github.com/tencentcloud/tencentcloud-cls-sdk-go"
	"time"
	"unsafe"
)

var ProducerInstance *cls.AsyncProducerClient
var callBack = &Callback{}
var TopicId = ""

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	// Gets called only once when the plugin.so is loaded
	return output.FLBPluginRegister(ctx, "fluent-bit-go-cls", "fluent-bit-go-cls")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	producerConfig := cls.GetDefaultAsyncProducerClientConfig()
	producerConfig.Endpoint = output.FLBPluginConfigKey(plugin, "CLSEndPoint")
	producerConfig.AccessKeyID = output.FLBPluginConfigKey(plugin, "AccessKeyID")
	producerConfig.AccessKeySecret = output.FLBPluginConfigKey(plugin, "AccessKeySecret")
	producerConfig.LingerMs = 200
	producerConfig.TotalSizeLnBytes = 500 * 1024 * 1024
	producerConfig.Retries = 10
	TopicId = output.FLBPluginConfigKey(plugin, "TopicID")

	var err error
	ProducerInstance, err = cls.NewAsyncProducerClient(producerConfig)
	if err != nil {
		fmt.Printf("[error] cls log producer init failed, reason: [%s]\n", err.Error())
		return output.FLB_ERROR
	}
	// 异步发送程序，需要启动
	ProducerInstance.Start()
	fmt.Printf("[info] cls log producer init success \n")
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, _ *C.char) int {
	// Gets called with a batch of records to be written to an instance.
	if ProducerInstance == nil {
		fmt.Printf("[error] cls log producer is nil \n")
		return output.FLB_ERROR
	}
	logs := make([]*cls.Log, 0)
	dec := output.NewDecoder(data, int(length))
	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}
		var logTime time.Time
		switch t := ts.(type) {
		case output.FLBTime:
			logTime = ts.(output.FLBTime).Time
		case uint64:
			logTime = time.Unix(int64(t), 0)
		default:
			fmt.Println("[warn] unknown timestamp format.")
			logTime = time.Now()
		}

		contents := make(map[string]string, len(record))
		for k, v := range record {
			k, _ := k.(string)
			switch t := v.(type) {
			case string:
				contents[k] = t
			case []byte:
				contents[k] = string(t)
			default:
				contents[k] = fmt.Sprintf("%v", v)
			}
		}

		if len(contents) == 0 {
			continue
		}
		log := cls.NewCLSLog(logTime.Unix(), contents)
		logs = append(logs, log)
	}
	if len(logs) == 0 {
		return output.FLB_OK
	}
	err := ProducerInstance.SendLogList(TopicId, logs, callBack)
	if err != nil {
		fmt.Printf("[error] cls log produce [%s] putlogs fail,, err: %s\n", TopicId, err.Error())
		return output.FLB_ERROR
	}
	// output.FLB_OK    = The data have been processed normally.
	// output.FLB_ERROR = An internal error have ocurred, the plugin will not handle the set of records/data again.
	// output.FLB_RETRY = A recoverable error have ocurred, the engine can try to flush the records/data later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

type Callback struct {
}

func (callback *Callback) Success(_ *cls.Result) {
}

func (callback *Callback) Fail(result *cls.Result) {
	fmt.Printf("[error] cls log produce [%s] putlogs fail, request_id:[%s]. attempts: [%d], err: %s\n",
		TopicId, result.GetRequestId(), result.GetReservedAttempts(), result.GetErrorMessage())
}

func main() {
}
