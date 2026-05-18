# Go Migration Examples

Side-by-side code translations for Go developers migrating to Temporal. Go developers migrating to Temporal often come from custom job schedulers, cron-based scripts, or in-house workflow engines. This file covers those patterns plus the general enterprise scheduler patterns.

For SDK setup and Worker configuration, refer to the `temporal-developer` skill (`references/go/go.md`).

*Verified against: Temporal Go SDK 1.x*

---

## Cron Job / Scheduled Script → Temporal Schedule + Workflow

```go
// ── CRON / CUSTOM SCHEDULER ───────────────────────────────────────────────────
// main.go (typical Go cron approach)
func main() {
    c := cron.New()
    c.AddFunc("0 2 * * *", func() {
        if err := runNightlyETL(); err != nil {
            log.Errorf("Nightly ETL failed: %v", err)
            sendAlert(err)
        }
    })
    c.Start()
    select {} // run forever
}

func runNightlyETL() error {
    data, err := extractData()
    if err != nil {
        return err
    }
    transformed, err := transformData(data)
    if err != nil {
        return err
    }
    return loadData(transformed)
}


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// activities.go
package etl

import "context"

func ExtractData(ctx context.Context) ([]Record, error) {
    // ... extract logic
    return records, nil
}

func TransformData(ctx context.Context, records []Record) ([]Record, error) {
    // ... transform logic
    return transformed, nil
}

func LoadData(ctx context.Context, records []Record) error {
    // ... load logic
    return nil
}


// workflow.go
package etl

import (
    "time"
    "go.temporal.io/sdk/temporal"
    "go.temporal.io/sdk/workflow"
)

func NightlyETLWorkflow(ctx workflow.Context) error {
    opts := workflow.ActivityOptions{
        StartToCloseTimeout: 2 * time.Hour,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 3,
            InitialInterval: 30 * time.Second,
        },
    }
    ctx = workflow.WithActivityOptions(ctx, opts)

    var records []Record
    if err := workflow.ExecuteActivity(ctx, ExtractData).Get(ctx, &records); err != nil {
        return err
    }

    var transformed []Record
    if err := workflow.ExecuteActivity(ctx, TransformData, records).Get(ctx, &transformed); err != nil {
        return err
    }

    return workflow.ExecuteActivity(ctx, LoadData, transformed).Get(ctx, nil)
}


// schedule.go — create the schedule once (replaces cron.AddFunc)
func CreateNightlySchedule(ctx context.Context, client client.Client) error {
    scheduleClient := client.ScheduleClient()
    _, err := scheduleClient.Create(ctx, client.ScheduleOptions{
        ID: "nightly-etl",
        Spec: client.ScheduleSpec{
            CronExpressions: []string{"0 2 * * *"},
        },
        Action: &client.ScheduleWorkflowAction{
            Workflow:  NightlyETLWorkflow,
            TaskQueue: "etl",
        },
    })
    return err
}
```

---

## Job with Retries → Activity with RetryPolicy

```go
// ── CUSTOM RETRY LOOP ─────────────────────────────────────────────────────────
func runWithRetry(fn func() error, maxRetries int, backoff time.Duration) error {
    var err error
    for attempt := 0; attempt <= maxRetries; attempt++ {
        if err = fn(); err == nil {
            return nil
        }
        if attempt < maxRetries {
            time.Sleep(backoff * time.Duration(attempt+1))
        }
    }
    return fmt.Errorf("failed after %d retries: %w", maxRetries, err)
}

err := runWithRetry(func() error {
    return callExternalAPI(payload)
}, 3, 10*time.Second)


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
func CallExternalAPI(ctx context.Context, payload APIPayload) (APIResponse, error) {
    // Activity — no retry loop needed; Temporal handles retries
    resp, err := httpClient.Post(apiURL, payload)
    if err != nil {
        return APIResponse{}, err  // Temporal will retry based on RetryPolicy
    }
    return resp, nil
}

// In the workflow, configure retry at the call site:
opts := workflow.ActivityOptions{
    StartToCloseTimeout: 30 * time.Second,
    RetryPolicy: &temporal.RetryPolicy{
        MaximumAttempts:        4,
        InitialInterval:        10 * time.Second,
        BackoffCoefficient:     2.0,
        NonRetryableErrorTypes: []string{"ValidationError"},
    },
}
var result APIResponse
workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), CallExternalAPI, payload).Get(ctx, &result)
```

---

## Parallel Jobs → workflow.Go + Futures

```go
// ── GOROUTINES WITH WAITGROUP ─────────────────────────────────────────────────
var wg sync.WaitGroup
results := make([]Result, 3)
errs := make([]error, 3)

for i, region := range []string{"us-east", "eu-west", "ap-south"} {
    wg.Add(1)
    go func(idx int, r string) {
        defer wg.Done()
        results[idx], errs[idx] = processRegion(r)
    }(i, region)
}
wg.Wait()


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
func RegionProcessingWorkflow(ctx workflow.Context) error {
    regions := []string{"us-east", "eu-west", "ap-south"}
    opts := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Minute}
    ctx = workflow.WithActivityOptions(ctx, opts)

    // Fan out: one future per region
    futures := make([]workflow.Future, len(regions))
    for i, region := range regions {
        futures[i] = workflow.ExecuteActivity(ctx, ProcessRegion, region)
    }

    // Fan in: collect all results
    results := make([]Result, len(regions))
    for i, future := range futures {
        if err := future.Get(ctx, &results[i]); err != nil {
            return fmt.Errorf("region %s failed: %w", regions[i], err)
        }
    }

    return workflow.ExecuteActivity(ctx, AggregateResults, results).Get(ctx, nil)
}
```

---

## Long-Running Polling Loop → Activity with Heartbeat

```go
// ── POLLING LOOP ──────────────────────────────────────────────────────────────
func waitForFileReady(ctx context.Context, path string) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            if _, err := os.Stat(path); err == nil {
                return nil
            }
            time.Sleep(30 * time.Second)
        }
    }
}


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
func WaitForFile(ctx context.Context, path string) error {
    for {
        if _, err := os.Stat(path); err == nil {
            return nil  // file found
        }
        // Heartbeat: tells Temporal we're still alive
        // If heartbeat stops (worker dies), Temporal retries the activity
        activity.RecordHeartbeat(ctx, fmt.Sprintf("waiting for %s", path))

        select {
        case <-time.After(30 * time.Second):
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}

// Called with heartbeat_timeout so a dead worker is detected quickly:
opts := workflow.ActivityOptions{
    StartToCloseTimeout: 4 * time.Hour,
    HeartbeatTimeout:    2 * time.Minute,
}
workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), WaitForFile, filePath).Get(ctx, nil)
```

---

## Hold / Pause → Signal + workflow.Await

```go
// ── MANUAL PAUSE (e.g., Control-M Hold/Free pattern) ─────────────────────────
// Typically requires a custom flag in a database + polling in the job


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// workflow.go
var ReleaseSignal = "release"

func HoldableWorkflow(ctx workflow.Context, config JobConfig) error {
    released := false

    // Register signal handler
    workflow.SetSignalChannel(ctx, ReleaseSignal).Receive(ctx, nil)  // simplified
    // Or use a proper signal handler:
    workflow.Go(ctx, func(ctx workflow.Context) {
        workflow.GetSignalChannel(ctx, ReleaseSignal).Receive(ctx, nil)
        released = true
    })

    // Wait until released
    _ = workflow.Await(ctx, func() bool { return released })

    opts := workflow.ActivityOptions{StartToCloseTimeout: 2 * time.Hour}
    return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), RunBatchJob, config).Get(ctx, nil)
}

// Send the release signal (replaces "Free" button in Control-M):
// workflowRun.SignalWorkflow(ctx, ReleaseSignal, nil)
```

---

## Compensation / Saga Pattern

```go
// ── TEMPORAL: Saga in Go ──────────────────────────────────────────────────────
func BookingWorkflow(ctx workflow.Context, req BookingRequest) error {
    opts := workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Minute}
    ctx = workflow.WithActivityOptions(ctx, opts)

    // Track compensations in order (run in reverse on failure)
    var compensations []func(workflow.Context) error

    if err := workflow.ExecuteActivity(ctx, BookFlight, req).Get(ctx, nil); err != nil {
        return err
    }
    compensations = append(compensations, func(ctx workflow.Context) error {
        return workflow.ExecuteActivity(ctx, CancelFlight, req).Get(ctx, nil)
    })

    if err := workflow.ExecuteActivity(ctx, BookHotel, req).Get(ctx, nil); err != nil {
        runCompensations(ctx, compensations)
        return err
    }
    compensations = append(compensations, func(ctx workflow.Context) error {
        return workflow.ExecuteActivity(ctx, CancelHotel, req).Get(ctx, nil)
    })

    if err := workflow.ExecuteActivity(ctx, ChargeCard, req).Get(ctx, nil); err != nil {
        runCompensations(ctx, compensations)
        return err
    }

    return nil
}

func runCompensations(ctx workflow.Context, compensations []func(workflow.Context) error) {
    // Run in reverse order
    for i := len(compensations) - 1; i >= 0; i-- {
        compensations[i](ctx)  // best-effort; log errors
    }
}
```

---

## Continue-As-New for Long-Running Loops

```go
// ── PATTERN: infinite poller / recurring workflow ─────────────────────────────
// Equivalent to a cyclic job in Control-M or a while-true loop

const MaxIterationsPerRun = 1000

type PollerState struct {
    IterationCount int
    Config         PollerConfig
}

func PollerWorkflow(ctx workflow.Context, state PollerState) error {
    opts := workflow.ActivityOptions{
        StartToCloseTimeout: 5 * time.Minute,
        HeartbeatTimeout:    1 * time.Minute,
    }
    ctx = workflow.WithActivityOptions(ctx, opts)

    for i := 0; i < MaxIterationsPerRun; i++ {
        if err := workflow.ExecuteActivity(ctx, PollAndProcess, state.Config).Get(ctx, nil); err != nil {
            return err
        }
        state.IterationCount++
        _ = workflow.Sleep(ctx, 1*time.Minute)
    }

    // Continue as new: start a fresh execution with clean history
    return workflow.NewContinueAsNewError(ctx, PollerWorkflow, state)
}
```
