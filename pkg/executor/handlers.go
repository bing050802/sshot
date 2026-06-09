package executor

import (
	"fmt"
	"os"

	"github.com/fgouteroux/sshot/pkg/types"
	"github.com/fgouteroux/sshot/pkg/utils"
)

// ExecuteHandlers 执行所有待执行的 handlers
func (e *Executor) ExecuteHandlers(handlers []types.Task) error {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	e.mu.Lock()
	queue := make([]string, len(e.NotifyQueue))
	copy(queue, e.NotifyQueue)
	e.NotifyQueue = make([]string, 0)
	e.mu.Unlock()

	if len(queue) == 0 {
		return nil
	}

	fmt.Fprintf(writer, "\n  %sRunning handlers...%s\n",
		utils.Color(utils.ColorCyan), utils.Color(utils.ColorReset))

	for _, handlerName := range queue {
		// 查找 handler
		var handler *types.Task
		for i, h := range handlers {
			if h.Name == handlerName {
				handler = &handlers[i]
				break
			}
		}

		if handler == nil {
			return fmt.Errorf("handler '%s' not found", handlerName)
		}

		// 检查是否已执行
		e.mu.Lock()
		if e.HandlersDone[handlerName] {
			e.mu.Unlock()
			if types.ExecOptions.Verbose {
				fmt.Fprintf(writer, "    │ Handler '%s' already executed, skipping\n", handlerName)
			}
			continue
		}
		e.mu.Unlock()

		// 执行 handler
		if types.ExecOptions.Verbose {
			fmt.Fprintf(writer, "    │ Running handler: %s\n", handlerName)
		}

		// 执行 handler 任务（不使用 notify，避免无限循环）
		err := e.executeHandlerTask(handler)

		e.mu.Lock()
		e.HandlersDone[handlerName] = true
		e.mu.Unlock()

		if err != nil {
			return fmt.Errorf("handler '%s' failed: %w", handlerName, err)
		}

		fmt.Fprintf(writer, "    %s✓ Handler '%s' completed%s\n",
			utils.Color(utils.ColorGreen), handlerName, utils.Color(utils.ColorReset))
	}

	return nil
}

// executeHandlerTask 执行 handler 任务（不触发新的 notify）
func (e *Executor) executeHandlerTask(task *types.Task) error {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	var output string
	var err error

	switch {
	case task.Command != "":
		output, err = e.executeCommand(task.Command, task.Sudo)
	case task.Shell != "":
		output, err = e.executeCommand(task.Shell, task.Sudo)
	case task.Systemd != nil:
		output, err = e.executeSystemd(task.Systemd)
	case task.Copy != nil:
		output, err = e.executeCopy(task.Copy)
	case task.File != nil:
		output, err = e.executeFile(task.File)
	default:
		return fmt.Errorf("handler has no executable action")
	}

	if err != nil {
		return err
	}

	if types.ExecOptions.Verbose && output != "" {
		e.mu.Lock()
		e.printOutput(writer, output)
		e.mu.Unlock()
	}

	return nil
}
