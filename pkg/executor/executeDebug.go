package executor

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/fgouteroux/sshot/pkg/types"
	"github.com/fgouteroux/sshot/pkg/utils"
)

// executeDebug 输出调试信息
func (e *Executor) executeDebug(debugTask *types.DebugTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	var output string
	if debugTask.Msg != "" {
		// 替换消息中的变量
		output = e.SubstituteVars(debugTask.Msg)
	} else if debugTask.Var != "" {
		// 获取变量的值并格式化输出
		val, err := e.getVariableValue(debugTask.Var)
		if err != nil {
			output = fmt.Sprintf("VARIABLE IS NOT DEFINED: %s", debugTask.Var)
		} else {
			// 格式化输出，如果是复合类型则输出 JSON
			output = formatDebugValue(val)
		}
	} else {
		return "", fmt.Errorf("debug task requires either msg or var")
	}

	// 输出到控制台（带颜色标识）
	e.mu.Lock()
	fmt.Fprintf(writer, "  %s📢 Debug:%s %s\n", utils.Color(utils.ColorCyan), utils.Color(utils.ColorReset), output)
	e.mu.Unlock()

	return output, nil
}

// getVariableValue 从多个来源查找变量值
func (e *Executor) getVariableValue(varName string) (interface{}, error) {
	// 1. 先从 Registers 查找（任务注册输出）
	if val, ok := e.Registers[varName]; ok {
		return val, nil
	}
	// 2. 从 Variables 查找（facts、set_fact 等）
	if val, ok := e.Variables[varName]; ok {
		return val, nil
	}
	// 3. 从 Host.Vars 查找（inventory 中定义的主机变量）
	if val, ok := e.Host.Vars[varName]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("variable %s not found", varName)
}

// formatDebugValue 格式化输出调试值
func formatDebugValue(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case []interface{}, map[string]interface{}:
		// 复杂类型转为 JSON 字符串
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	default:
		return fmt.Sprintf("%v", v)
	}
}
