// Package task executes work on the local machine on behalf of a remote peer.
// Three kinds exist in the prototype:
//
//	shell   - run a command, stream stdout/stderr, return the exit code
//	compute - count primes below N by trial division, streaming progress
//	payload - (handled by the agent) hash a byte stream as it arrives
package task

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Output receives streamed output. stream is "stdout", "stderr" or "progress".
type Output func(stream, text string)

type Result struct {
	ExitCode int
	Err      string
	Summary  string
}

// RunShell runs command through the platform shell. Cancelling ctx kills the
// whole process tree (cmd.exe children included on Windows).
func RunShell(ctx context.Context, command string, out Output) Result {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		// chcp 65001 makes cmd emit UTF-8 so non-ASCII output survives the trip.
		cmd = exec.CommandContext(ctx, "cmd", "/C", "chcp 65001 >nul && "+command)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", command)
	}
	cmd.WaitDelay = 2 * time.Second
	cmd.Cancel = func() error { return killTree(cmd) }

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return Result{ExitCode: -1, Err: err.Error()}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return Result{ExitCode: -1, Err: err.Error()}
	}
	if err := cmd.Start(); err != nil {
		return Result{ExitCode: -1, Err: err.Error()}
	}

	var wg sync.WaitGroup
	pump := func(r io.Reader, name string) {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				out(name, string(buf[:n]))
			}
			if err != nil {
				return
			}
		}
	}
	wg.Add(2)
	go pump(stdout, "stdout")
	go pump(stderr, "stderr")
	wg.Wait()

	werr := cmd.Wait()
	if ctx.Err() != nil {
		return Result{ExitCode: -1, Err: "cancelled: " + ctx.Err().Error()}
	}
	if werr != nil {
		var ee *exec.ExitError
		if errors.As(werr, &ee) {
			return Result{ExitCode: ee.ExitCode()}
		}
		return Result{ExitCode: -1, Err: werr.Error()}
	}
	return Result{ExitCode: 0}
}

func killTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	if runtime.GOOS == "windows" {
		// cmd.exe does not forward signals to its children; taskkill /T does.
		_ = exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid)).Run()
		return nil
	}
	return cmd.Process.Kill()
}

// RunPrimes counts primes below n by trial division. It is deliberately
// CPU-bound and slow enough to be visible (a few seconds for n = 5,000,000),
// streams progress every 10%, and returns a value that is easy to verify:
//
//	n = 1,000,000  ->    78,498
//	n = 2,000,000  ->   148,933
//	n = 5,000,000  ->   348,513
//	n = 10,000,000 ->   664,579
func RunPrimes(ctx context.Context, n int64, out Output) Result {
	if n < 2 {
		n = 2
	}
	start := time.Now()
	var count int64
	next := n / 10
	if next == 0 {
		next = n
	}
	for i := int64(2); i < n; i++ {
		if i >= next {
			out("progress", fmt.Sprintf("%3d%%  %d primes so far\n", i*100/n, count))
			next += n / 10
		}
		if i&0xFFFF == 0 && ctx.Err() != nil {
			return Result{ExitCode: -1, Err: "cancelled: " + ctx.Err().Error()}
		}
		if isPrime(i) {
			count++
		}
	}
	d := time.Since(start)
	out("progress", fmt.Sprintf("100%%  done in %.2fs\n", d.Seconds()))
	return Result{ExitCode: 0, Summary: fmt.Sprintf("primes below %d = %d  (%.2fs on %s/%s)", n, count, d.Seconds(), runtime.GOOS, runtime.GOARCH)}
}

func isPrime(n int64) bool {
	if n < 2 {
		return false
	}
	if n%2 == 0 {
		return n == 2
	}
	for d := int64(3); d*d <= n; d += 2 {
		if n%d == 0 {
			return false
		}
	}
	return true
}
