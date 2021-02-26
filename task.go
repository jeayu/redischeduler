package redischeduler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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

func (t *WorkerTask) Deserialization() (task *Task, err error) {
	b, err := base64.StdEncoding.DecodeString(t.TaskId)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &task)
	if err != nil {
		return nil, err
	}
	return task, err
}

type Invoker interface {
	Call(task WorkerTask) (err error)
}

type TaskInvoker struct {
	Functions map[string]reflect.Value
}

func (i *TaskInvoker) Call(workerTask WorkerTask) (err error) {
	task, err := workerTask.Deserialization()
	if err != nil {
		return err
	}
	if f, ok := i.Functions[task.Function]; ok {
		args := make([]reflect.Value, len(task.Args))
		for i, a := range task.Args {
			args[i] = reflect.ValueOf(a)
		}
		defer func() {
			if r := recover(); r != nil {
				err = errors.New(fmt.Sprintf("Invoker task %v error: %v", task, r))
			}
		}()
		f.Call(args)
	} else {
		err = errors.New(fmt.Sprintf("Invoker task %v error: Function not found!", task))
	}
	return err
}
