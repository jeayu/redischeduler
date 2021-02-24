package redischeduler

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
)

type Task struct {
	Function string
	Args     []interface{}
}

func NewTask(function string, args ...interface{}) *Task {
	return &Task{
		Function: function,
		Args:     args,
	}
}

func (t *Task) Serialization() string {
	data, _ := json.Marshal(t)
	dataBase64 := base64.StdEncoding.EncodeToString(data)
	return dataBase64
}

type WorkerTask struct {
	TaskId string
}

func NewWorkerTask(taskId string) *WorkerTask {
	return &WorkerTask{taskId}
}

func (t *WorkerTask) Deserialization() *Task {
	b, err := base64.StdEncoding.DecodeString(t.TaskId)
	if err != nil {
		return nil
	}
	task := &Task{}
	err = json.Unmarshal(b, task)
	if err != nil {
		return nil
	}
	return task
}

type Invoker interface {
	Call(task WorkerTask) bool
}

type TaskInvoker struct {
	functions map[string]reflect.Value
}

func (i *TaskInvoker) Call(workerTask WorkerTask) bool {
	task := workerTask.Deserialization()
	if f, ok := i.functions[task.Function]; ok {
		args := make([]reflect.Value, len(task.Args))
		for i, a := range task.Args {
			args[i] = reflect.ValueOf(a)
		}
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Task %v error: %v\n", task, r)
			}
		}()
		return f.Call(args) == nil
	} else {
		fmt.Printf("Task %v function not found!\n", task)
		return false
	}
}
