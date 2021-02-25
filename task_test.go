package redischeduler

import (
	"fmt"
	"reflect"
	"testing"
)

type service struct {
}

func (h *service) SayHi(name string) {
	fmt.Println("Hi!", name)
}
func (h *service) SayHello(name string) {
	fmt.Println("Hello!", name)
}

func (h *service) Say(v ...interface{}) {
	fmt.Println("Say:", v)
}

func HaHa(name string) {
	fmt.Println("HaHa:", name)
}

func speechless() {
	fmt.Println("speechless")
}

func TestTask(t *testing.T) {
	s := &service{}
	taskInvoker := &TaskInvoker{
		Functions: map[string]reflect.Value{
			"SayHi":      reflect.ValueOf(s.SayHi),
			"SayHello":   reflect.ValueOf(s.SayHello),
			"Say":        reflect.ValueOf(s.Say),
			"Haha":       reflect.ValueOf(HaHa),
			"speechless": reflect.ValueOf(speechless),
		},
	}
	task := NewTask("SayHi", "world")
	taskId := task.Serialization()
	workerTask := NewWorkerTask(taskId)
	err := taskInvoker.Call(*workerTask)
	if err != nil {
		t.Fatalf("Task %v fail! err:%v\n", task, err)
	}

	task = NewTask("SayHello", "world")
	taskId = task.Serialization()
	workerTask = &WorkerTask{taskId}
	err = taskInvoker.Call(*workerTask)
	if err != nil {
		t.Fatalf("Task %v fail! err:%v\n", task, err)
	}

	task = NewTask("Say", "world", 1, true)
	taskId = task.Serialization()
	workerTask = &WorkerTask{taskId}
	err = taskInvoker.Call(*workerTask)
	if err != nil {
		t.Fatalf("Task %v fail! err:%v\n", task, err)
	}

	task = NewTask("Haha", "world")
	taskId = task.Serialization()
	workerTask = &WorkerTask{taskId}
	err = taskInvoker.Call(*workerTask)
	if err != nil {
		t.Fatalf("Task %v fail! err:%v\n", task, err)
	}

	task = NewTask("speechless")
	taskId = task.Serialization()
	workerTask = &WorkerTask{taskId}
	err = taskInvoker.Call(*workerTask)
	if err != nil {
		t.Fatalf("Task %v fail! err:%v\n", task, err)
	}

}

func TestTaskInvokerError(t *testing.T) {
	taskInvoker := &TaskInvoker{
		Functions: map[string]reflect.Value{
			"HaHa":       reflect.ValueOf(HaHa),
			"speechless": reflect.ValueOf(speechless),
		},
	}
	task := NewTask("SayHello", "world")
	taskId := task.Serialization()
	workerTask := NewWorkerTask(taskId)
	err := taskInvoker.Call(*workerTask)
	if err != nil {
		t.Logf("Task %v fail! err:%v\n", task, err)
	}

	task = NewTask("HaHa", "world", 1, 2, 3)
	taskId = task.Serialization()
	workerTask = NewWorkerTask(taskId)
	err = taskInvoker.Call(*workerTask)
	if err != nil {
		t.Logf("Task %v fail! err:%v\n", task, err)
	}
}

type customInvoker struct {
	function func(args ...interface{})
}

func (i *customInvoker) Call(workerTask WorkerTask) error {
	task := workerTask.Deserialization()
	i.function(task.Args)
	return nil
}

func TestTaskInvoker(t *testing.T) {
	invoker := &customInvoker{
		function: func(args ...interface{}) {
			fmt.Println("invoke args:", args)
		},
	}
	task := NewTask("test", "world", 1, true)
	taskId := task.Serialization()
	workerTask := NewWorkerTask(taskId)
	err := invoker.Call(*workerTask)
	if err != nil {
		t.Fatalf("Task %v fail! err:%v\n", task, err)
	}
}
