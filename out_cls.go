package main

import "C"
import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	cls "github.com/tencentcloud/tencentcloud-cls-sdk-go"
)

// PluginContext holds instance-specific data
type PluginContext struct {
	ProducerInstance *cls.AsyncProducerClient
	TopicId          string
	callBack         *Callback
}

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
	topicId := output.FLBPluginConfigKey(plugin, "TopicID")

	var err error
	producerInstance, err := cls.NewAsyncProducerClient(producerConfig)
	if err != nil {
		fmt.Printf("[error] cls log producer init failed, reason: [%s]\n", err.Error())
		return output.FLB_ERROR
	}
	// 异步发送程序，需要启动
	producerInstance.Start()

	// Create plugin context with instance-specific data
	pluginContext := &PluginContext{
		ProducerInstance: producerInstance,
		TopicId:          topicId,
		callBack:         &Callback{TopicId: topicId},
	}

	// Store context in plugin instance
	output.FLBPluginSetContext(plugin, pluginContext)

	fmt.Printf("[info] cls log producer init success for topic: %s\n", topicId)
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, _ *C.char) int {
	// Gets called with a batch of records to be written to an instance.
	pluginContextInterface := output.FLBPluginGetContext(ctx)
	if pluginContextInterface == nil {
		fmt.Printf("[error] cls log producer context is nil \n")
		return output.FLB_ERROR
	}

	pluginContext, ok := pluginContextInterface.(*PluginContext)
	if !ok || pluginContext.ProducerInstance == nil {
		fmt.Printf("[error] cls log producer is nil or invalid context \n")
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
			contents[stringifyKey(k)] = stringifyValue(v)
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
	err := pluginContext.ProducerInstance.SendLogList(pluginContext.TopicId, logs, pluginContext.callBack)
	if err != nil {
		fmt.Printf("[error] cls log produce [%s] putlogs fail, err: %s\n", pluginContext.TopicId, err.Error())
		return output.FLB_RETRY
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

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	pluginContextInterface := output.FLBPluginGetContext(ctx)
	if pluginContextInterface == nil {
		return output.FLB_OK
	}

	pluginContext, ok := pluginContextInterface.(*PluginContext)
	if !ok || pluginContext.ProducerInstance == nil {
		return output.FLB_OK
	}

	// 等待异步producer发送完缓冲区中的日志
	if err := pluginContext.ProducerInstance.Close(30000); err != nil {
		fmt.Printf("[error] cls log producer close failed for topic [%s], err: %s\n", pluginContext.TopicId, err.Error())
		return output.FLB_ERROR
	}

	fmt.Printf("[info] cls log producer closed for topic: %s\n", pluginContext.TopicId)
	return output.FLB_OK
}

type Callback struct {
	TopicId string
}

func (callback *Callback) Success(_ *cls.Result) {
}

func (callback *Callback) Fail(result *cls.Result) {
	fmt.Printf("[error] cls log produce [%s] putlogs fail, request_id:[%s]. attempts: [%+v], err: %s\n",
		callback.TopicId, result.GetRequestId(), result.GetReservedAttempts(), result.GetErrorMessage())
}

func main() {
}

func stringifyKey(key interface{}) string {
	switch k := key.(type) {
	case string:
		return k
	case []byte:
		return string(k)
	default:
		return fmt.Sprintf("%v", key)
	}
}

func stringifyValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}

	if !isStructuredValue(value) {
		return fmt.Sprintf("%v", value)
	}

	data, err := json.Marshal(normalizeJSONValue(value))
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(data)
}

func isStructuredValue(value interface{}) bool {
	if value == nil {
		return false
	}

	valueType := reflect.TypeOf(value)
	switch valueType.Kind() {
	case reflect.Map:
		return true
	case reflect.Slice, reflect.Array:
		return valueType.Elem().Kind() != reflect.Uint8
	default:
		return false
	}
}

func normalizeJSONValue(value interface{}) interface{} {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		return v
	case []byte:
		return string(v)
	}

	valueRef := reflect.ValueOf(value)
	switch valueRef.Kind() {
	case reflect.Map:
		out := make(map[string]interface{}, valueRef.Len())
		iter := valueRef.MapRange()
		for iter.Next() {
			out[stringifyKey(iter.Key().Interface())] = normalizeJSONValue(iter.Value().Interface())
		}
		return out
	case reflect.Slice, reflect.Array:
		out := make([]interface{}, valueRef.Len())
		for i := 0; i < valueRef.Len(); i++ {
			out[i] = normalizeJSONValue(valueRef.Index(i).Interface())
		}
		return out
	default:
		return value
	}
}
