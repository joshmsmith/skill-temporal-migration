# .NET / C# Migration Examples

Side-by-side code translations for .NET developers migrating to Temporal. Primary source tools covered: **Hangfire**, **Windows Task Scheduler / Azure Functions timer triggers**, **Quartz.NET**, and general enterprise BPM patterns.

For SDK setup and Worker configuration, refer to the `temporal-developer` skill (`references/dotnet/dotnet.md`).

*Verified against: Temporal .NET SDK 1.x, Hangfire 1.8.x, Quartz.NET 3.x*

---

## Hangfire Background Job → Temporal Activity + Workflow

```csharp
// ── HANGFIRE ─────────────────────────────────────────────────────────────────
// Fire-and-forget job
BackgroundJob.Enqueue(() => emailService.SendWelcomeEmail(userId));

// Delayed job
BackgroundJob.Schedule(() => emailService.SendFollowup(userId), TimeSpan.FromDays(3));

// Recurring job
RecurringJob.AddOrUpdate("nightly-report", () => reportService.GenerateDaily(), Cron.Daily(2));

// Job chaining
var jobId = BackgroundJob.Enqueue(() => orderService.Reserve(orderId));
BackgroundJob.ContinueJobWith(jobId, () => orderService.Charge(orderId));
BackgroundJob.ContinueJobWith(jobId, () => orderService.Ship(orderId));


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// Activities (one per unit of work — replaces Hangfire job methods)
[Activity]
public class OrderActivities
{
    private readonly IOrderService _orderService;
    private readonly IEmailService _emailService;

    public OrderActivities(IOrderService orderService, IEmailService emailService)
    {
        _orderService = orderService;
        _emailService = emailService;
    }

    [ActivityMethod]
    public async Task SendWelcomeEmail(string userId) =>
        await _emailService.SendWelcomeEmailAsync(userId);

    [ActivityMethod]
    public async Task SendFollowup(string userId) =>
        await _emailService.SendFollowupAsync(userId);

    [ActivityMethod]
    public async Task ReserveInventory(string orderId) =>
        await _orderService.ReserveAsync(orderId);

    [ActivityMethod]
    public async Task ChargePayment(string orderId) =>
        await _orderService.ChargeAsync(orderId);

    [ActivityMethod]
    public async Task ShipOrder(string orderId) =>
        await _orderService.ShipAsync(orderId);
}


// Workflow (replaces job chaining — the sequence lives here in code)
[Workflow]
public class OnboardingWorkflow
{
    [WorkflowRun]
    public async Task RunAsync(string userId)
    {
        var activities = Workflow.CreateActivityHandle<OrderActivities>(
            new() { StartToCloseTimeout = TimeSpan.FromMinutes(5) });

        await activities.SendWelcomeEmail(userId);
        await Workflow.DelayAsync(TimeSpan.FromDays(3));   // replaces BackgroundJob.Schedule
        await activities.SendFollowup(userId);
    }
}


[Workflow]
public class OrderWorkflow
{
    [WorkflowRun]
    public async Task RunAsync(string orderId)
    {
        var activities = Workflow.CreateActivityHandle<OrderActivities>(
            new() { StartToCloseTimeout = TimeSpan.FromMinutes(5) });

        // Replaces chained Hangfire jobs — runs in guaranteed sequence
        await activities.ReserveInventory(orderId);
        await activities.ChargePayment(orderId);
        await activities.ShipOrder(orderId);
    }
}


// Enqueueing a workflow (replaces BackgroundJob.Enqueue):
await temporalClient.StartWorkflowAsync(
    (OrderWorkflow wf) => wf.RunAsync(orderId),
    new WorkflowOptions
    {
        Id = $"order-{orderId}",
        TaskQueue = "orders",
    });


// Schedule (replaces RecurringJob.AddOrUpdate):
await scheduleClient.CreateScheduleAsync(
    "nightly-report",
    new Schedule
    {
        Action = new ScheduleActionStartWorkflow<NightlyReportWorkflow>(
            wf => wf.RunAsync(),
            new WorkflowOptions { TaskQueue = "reports" }),
        Spec = new ScheduleSpec
        {
            CronExpressions = { "0 2 * * *" },
        },
    });
```

---

## Quartz.NET: Job → Activity + Workflow + Schedule

```csharp
// ── QUARTZ.NET ────────────────────────────────────────────────────────────────
// Job class
public class SendReportJob : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        var reportType = context.MergedJobDataMap.GetString("reportType");
        await reportService.GenerateAsync(reportType);
        await emailService.SendAsync(reportType);
    }
}

// Scheduling it
IJobDetail job = JobBuilder.Create<SendReportJob>()
    .WithIdentity("sendReport")
    .UsingJobData("reportType", "daily-summary")
    .Build();

ITrigger trigger = TriggerBuilder.Create()
    .WithCronSchedule("0 0 8 * * ?")   // 8am daily
    .Build();

await scheduler.ScheduleJob(job, trigger);


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// Activities (replaces Job.Execute body — split into discrete units)
[Activity]
public class ReportActivities
{
    [ActivityMethod]
    public async Task GenerateReport(string reportType) =>
        await reportService.GenerateAsync(reportType);

    [ActivityMethod]
    public async Task SendReport(string reportType) =>
        await emailService.SendAsync(reportType);
}


// Workflow (orchestrates the activities)
[Workflow]
public class SendReportWorkflow
{
    [WorkflowRun]
    public async Task RunAsync(string reportType)
    {
        var activities = Workflow.CreateActivityHandle<ReportActivities>(
            new() { StartToCloseTimeout = TimeSpan.FromHours(1) });

        await activities.GenerateReport(reportType);
        await activities.SendReport(reportType);
    }
}


// Schedule (replaces scheduler.ScheduleJob + CronTrigger)
await scheduleClient.CreateScheduleAsync(
    "daily-report",
    new Schedule
    {
        Action = new ScheduleActionStartWorkflow<SendReportWorkflow>(
            wf => wf.RunAsync("daily-summary"),
            new WorkflowOptions { TaskQueue = "reports" }),
        Spec = new ScheduleSpec
        {
            CronExpressions = { "0 8 * * *" },  // 8am daily
        },
        Policy = new SchedulePolicy
        {
            Overlap = ScheduleOverlapPolicy.Skip,  // replaces @DisallowConcurrentExecution
        },
    });
```

---

## Windows Task Scheduler / Azure Functions Timer → Temporal Schedule

```csharp
// ── AZURE FUNCTIONS: Timer Trigger ────────────────────────────────────────────
[FunctionName("NightlyCleanup")]
public async Task Run([TimerTrigger("0 0 3 * * *")] TimerInfo timer, ILogger log)
{
    log.LogInformation("Starting nightly cleanup");
    await cleanupService.RunAsync();
}


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
[Activity]
public class CleanupActivities
{
    [ActivityMethod]
    public async Task RunCleanup() => await cleanupService.RunAsync();
}

[Workflow]
public class NightlyCleanupWorkflow
{
    [WorkflowRun]
    public async Task RunAsync()
    {
        var activities = Workflow.CreateActivityHandle<CleanupActivities>(
            new() { StartToCloseTimeout = TimeSpan.FromHours(2) });
        await activities.RunCleanup();
    }
}

// Schedule setup (run once at startup or via deployment script):
await scheduleClient.CreateScheduleAsync(
    "nightly-cleanup",
    new Schedule
    {
        Action = new ScheduleActionStartWorkflow<NightlyCleanupWorkflow>(
            wf => wf.RunAsync(),
            new WorkflowOptions { TaskQueue = "maintenance" }),
        Spec = new ScheduleSpec { CronExpressions = { "0 3 * * *" } },
    });
```

---

## Signal: Waiting for External Event

```csharp
// ── TEMPORAL: Signal handler (replaces polling / callback patterns) ────────────

[Workflow]
public class ApprovalWorkflow
{
    private TaskCompletionSource<ApprovalDecision> _approvalTcs = new();

    [WorkflowSignal]
    public async Task ApprovalReceived(ApprovalDecision decision)
    {
        _approvalTcs.TrySetResult(decision);
    }

    [WorkflowRun]
    public async Task<string> RunAsync(ApprovalRequest request)
    {
        var activities = Workflow.CreateActivityHandle<ApprovalActivities>(
            new() { StartToCloseTimeout = TimeSpan.FromMinutes(5) });

        await activities.SendForApproval(request);

        // Wait up to 7 days for approval signal
        var decision = await Workflow.WaitConditionAsync(
            () => _approvalTcs.Task.IsCompleted,
            TimeSpan.FromDays(7));

        if (!decision)  // timeout
        {
            await activities.EscalateApproval(request);
            return "ESCALATED";
        }

        var result = _approvalTcs.Task.Result;
        if (result.Approved)
        {
            await activities.ProcessApproved(request);
            return "APPROVED";
        }
        await activities.ProcessRejected(request);
        return "REJECTED";
    }
}

// Send signal from ASP.NET controller:
// var handle = temporalClient.GetWorkflowHandle<ApprovalWorkflow>($"approval-{requestId}");
// await handle.SignalAsync(wf => wf.ApprovalReceived(new ApprovalDecision { Approved = true, Reviewer = "alice" }));
```

---

## Compensation / Saga Pattern

```csharp
// ── TEMPORAL: Saga in C# ──────────────────────────────────────────────────────
[Workflow]
public class TravelBookingWorkflow
{
    [WorkflowRun]
    public async Task RunAsync(BookingRequest request)
    {
        var activities = Workflow.CreateActivityHandle<BookingActivities>(
            new() { StartToCloseTimeout = TimeSpan.FromMinutes(5) });

        var compensations = new Stack<Func<Task>>();

        try
        {
            await activities.BookFlight(request);
            compensations.Push(() => activities.CancelFlight(request));

            await activities.BookHotel(request);
            compensations.Push(() => activities.CancelHotel(request));

            await activities.ChargeCard(request);
            compensations.Push(() => activities.RefundCard(request));
        }
        catch
        {
            // Run compensations in reverse order
            foreach (var compensate in compensations)
            {
                try { await compensate(); }
                catch { /* log and continue */ }
            }
            throw;
        }
    }
}
```

---

## Retry Policy

```csharp
// ── HANGFIRE: Retry attribute ─────────────────────────────────────────────────
[AutomaticRetry(Attempts = 3, DelaysInSeconds = new[] { 10, 60, 300 })]
public async Task ProcessOrderAsync(string orderId) { ... }


// ── TEMPORAL: RetryPolicy on ActivityOptions ──────────────────────────────────
var activityOptions = new ActivityOptions
{
    StartToCloseTimeout = TimeSpan.FromMinutes(10),
    RetryPolicy = new RetryPolicy
    {
        MaximumAttempts = 3,
        InitialInterval = TimeSpan.FromSeconds(10),
        BackoffCoefficient = 2.0,
        MaximumInterval = TimeSpan.FromMinutes(5),
        NonRetryableErrorTypes = { nameof(InvalidOrderException) },
    },
};
var activities = Workflow.CreateActivityHandle<OrderActivities>(activityOptions);
await activities.ProcessOrder(orderId);
```
